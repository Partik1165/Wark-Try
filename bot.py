import logging
import json
import re
import uuid
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters, JobQueue
from telegram.error import TelegramError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv
import os
import shlex

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]
# Multiple MongoDB URIs: MONGO_URI_1, MONGO_URI_2, etc.
MONGO_URIS = {
    name.replace("MONGO_URI_", "").lower(): uri
    for name, uri in os.environ.items()
    if name.startswith("MONGO_URI_")
}
DEFAULT_DB = os.getenv("DEFAULT_DB", list(MONGO_URIS.keys())[0] if MONGO_URIS else None)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB setup for multiple databases
mongo_clients = {}
collections = {}
try:
    for db_name, uri in MONGO_URIS.items():
        client = MongoClient(uri, maxPoolSize=50)
        client.admin.command('ping')  # Test connection
        db = client["cricket_bot"]
        mongo_clients[db_name] = client
        collections[db_name] = {
            "matches": db["matches"],
            "user_teams": db["user_teams"],
            "points": db["points"],
            "amounts": db["amounts"],
            "yon_questions": db["yon_questions"],
            "yon_user_answers": db["yon_user_answers"],
            "yon_correct_answers": db["yon_correct_answers"],
            "locked_matches": db["locked_matches"],
            "notifications": db["notifications"]
        }
        # Create indexes for performance
        collections[db_name]["matches"].create_index("name", unique=True)
        collections[db_name]["user_teams"].create_index("user_id")
        collections[db_name]["yon_questions"].create_index("qid")
        collections[db_name]["locked_matches"].create_index("match_name")
        collections[db_name]["notifications"].create_index("timestamp")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

# In-memory cache for locked matches per database
locked_matches = {db_name: {doc["match_name"]: doc["locked"] for doc in collections[db_name]["locked_matches"].find()} for db_name in MONGO_URIS}

# Storage threshold (assuming 512 MB limit, 20 MB remaining = 492 MB used)
STORAGE_LIMIT_MB = 512
STORAGE_THRESHOLD_MB = STORAGE_LIMIT_MB - 20  # 492 MB
NOTIFICATION_COOLDOWN_MINUTES = 30

def is_admin(user_id):
    """Check if the user is an admin."""
    return user_id in ADMIN_IDS

def sanitize_input(text):
    """Remove potentially dangerous characters from input."""
    return re.sub(r'[^\w\s,.]', '', text.strip())

async def send_notification_to_admins(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send a message to all admins."""
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=message)
        except TelegramError as e:
            logger.error(f"Failed to send notification to admin {admin_id}: {e}")

async def check_storage(context: ContextTypes.DEFAULT_TYPE):
    """Check storage for each MongoDB database and notify admins if nearing limit."""
    for db_name in MONGO_URIS:
        try:
            db = mongo_clients[db_name]["cricket_bot"]
            stats = db.command("dbStats")
            data_size_mb = stats.get("dataSize", 0) / (1024 * 1024)  # Convert bytes to MB
            if data_size_mb >= STORAGE_THRESHOLD_MB:
                # Check last notification time
                last_notification = collections[db_name]["notifications"].find_one(
                    {"type": "storage_warning", "db_name": db_name},
                    sort=[("timestamp", -1)]
                )
                if last_notification:
                    last_time = last_notification["timestamp"]
                    if datetime.utcnow() - last_time < timedelta(minutes=NOTIFICATION_COOLDOWN_MINUTES):
                        continue
                # Send notification
                message = (
                    f"⚠️ Storage Warning for DB '{db_name}': "
                    f"{data_size_mb:.2f} MB used, nearing {STORAGE_LIMIT_MB} MB limit. "
                    f"Only {STORAGE_LIMIT_MB - data_size_mb:.2f} MB remaining!"
                )
                await send_notification_to_admins(context, message)
                # Record notification
                collections[db_name]["notifications"].insert_one({
                    "type": "storage_warning",
                    "db_name": db_name,
                    "message": message,
                    "timestamp": datetime.utcnow()
                })
        except PyMongoError as e:
            logger.error(f"Failed to check storage for DB {db_name}: {e}")

# === USER COMMANDS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message for users."""
    try:
        user = update.effective_user
        await update.message.reply_text(
            f"Hello {user.first_name}, welcome to the Cricket Team Selection Bot! "
            f"Use /schedule to get started, /profile to view your bets, /yon for Yes/No questions, or /help for commands."
        )
    except TelegramError as e:
        logger.error(f"Failed to send start message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user commands."""
    help_text = (
        "User Commands\n\n"
        "/start - Start the bot.\n"
        "/schedule [db_name] - View matches.\n"
        "/editteam [db_name] [match_name] - Edit team.\n"
        "/addamount [db_name] <match_name> <amount> - Set bet amount.\n"
        "/check [db_name] - View teams.\n"
        "/profile [db_name] - View teams and bets.\n"
        "/rankings [db_name] - See rankings.\n"
        "/yon [db_name] - Answer Yes/No questions.\n"
        "/yonrankings [db_name] - View Yes/No rankings.\n"
        "Admins: /admhelp for admin commands."
    )
    try:
        await update.message.reply_text(help_text)
    except TelegramError as e:
        logger.error(f"Failed to send help message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display available matches."""
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        matches = collections[db_name]["matches"].find()
        if not collections[db_name]["matches"].count_documents({}):
            await update.message.reply_text(f"No matches available in DB '{db_name}'.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"user_match_{db_name}::{m['name']}")] for m in matches]
        await update.message.reply_text(f"Select a match (DB: {db_name}):", reply_markup=InlineKeyboardMarkup(keyboard))
    except PyMongoError as e:
        logger.error(f"Failed to fetch matches: {e}")
        await update.message.reply_text("Error fetching matches.")
    except TelegramError as e:
        logger.error(f"Failed to send schedule message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def addamount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allow users to place a bet."""
    user_id = str(update.effective_user.id)
    try:
        if len(context.args) < 2:
            db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
            if db_name not in MONGO_URIS:
                await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            matches = collections[db_name]["matches"].find()
            if not collections[db_name]["matches"].count_documents({}):
                await update.message.reply_text(f"No matches available in DB '{db_name}'.")
                return
            keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"addamount::{db_name}::{m['name']}")] for m in matches]
            await update.message.reply_text(f"Select a match to set bet (DB: {db_name}):", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        db_name = DEFAULT_DB
        match_name = sanitize_input(context.args[0])
        amount_idx = 1
        if len(context.args) > 2:
            db_name = sanitize_input(context.args[0])
            match_name = sanitize_input(context.args[1])
            amount_idx = 2
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not match_name or not collections[db_name]["matches"].find_one({"name": match_name}):
            await update.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
            return
        if locked_matches[db_name].get(match_name, False):
            await update.message.reply_text(f"❌ This match is locked in DB '{db_name}'.")
            return
        try:
            amount = int(context.args[amount_idx])
            if amount <= 0:
                await update.message.reply_text("Enter a positive amount.")
                return
        except ValueError:
            await update.message.reply_text("Invalid amount (must be a number).")
            return
        collections[db_name]["amounts"].update_one(
            {"user_id": user_id},
            {"$set": {f"bets.{match_name}": amount}},
            upsert=True
        )
        await update.message.reply_text(
            f"Bet of {amount} points added for {match_name} in DB '{db_name}'. Tag @Trainer_OFFicial in the group."
        )
    except PyMongoError as e:
        logger.error(f"Failed to add amount: {e}")
        await update.message.reply_text("Error adding bet.")
    except TelegramError as e:
        logger.error(f"Failed to send addamount message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user's teams, bets, and Yes/No answers."""
    user_id = str(update.effective_user.id)
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        msg = f"📋 *Your Profile (DB: {db_name})* 📋\n\n"
        user_teams = collections[db_name]["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
        if not user_teams["teams"]:
            msg += "No teams selected.\n"
        else:
            msg += "Your Teams:\n"
            for match, players in user_teams["teams"].items():
                msg += f"{match}:\n"
                for i, p in enumerate(players):
                    role = " (Captain)" if i == 0 else " (Vice-Captain)" if i == 1 else ""
                    msg += f"- {p}{role}\n"
                msg += "\n"
        
        user_amounts = collections[db_name]["amounts"].find_one({"user_id": user_id}) or {"bets": {}}
        if not user_amounts["bets"]:
            msg += "No bets placed.\n"
        else:
            msg += "Your Bets:\n"
            for match, amount in user_amounts["bets"].items():
                msg += f"{match}: {amount} points\n"
        
        user_answers = collections[db_name]["yon_user_answers"].find_one({"user_id": user_id}) or {"answers": {}}
        if not user_answers["answers"]:
            msg += "No Yes/No answers.\n"
        else:
            msg += "Your Yes/No Answers:\n"
            for qid, answer in user_answers["answers"].items():
                question = collections[db_name]["yon_questions"].find_one({"qid": qid}) or {"question": "Unknown"}
                q_text = question["question"]
                msg += f"Q{qid}: {q_text} - {answer}\n"
        
        await update.message.reply_text(msg, parse_mode="Markdown")
    except PyMongoError as e:
        logger.error(f"Failed to fetch profile: {e}")
        await update.message.reply_text("Error fetching profile.")
    except TelegramError as e:
        logger.error(f"Failed to send profile message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user's teams."""
    user_id = str(update.effective_user.id)
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        user_teams = collections[db_name]["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
        if not user_teams["teams"]:
            await update.message.reply_text(f"No teams selected in DB '{db_name}'.")
            return
        msg = f"Your teams (DB: {db_name}):\n\n"
        for match, players in user_teams["teams"].items():
            msg += f"{match}:\n"
            for i, p in enumerate(players):
                role = " (Captain)" if i == 0 else " (Vice-Captain)" if i == 1 else ""
                msg += f"- {p}{role}\n"
            user_amounts = collections[db_name]["amounts"].find_one({"user_id": user_id}) or {"bets": {}}
            if match in user_amounts["bets"]:
                msg += f"Bet: {user_amounts['bets'][match]} points\n"
            msg += "\n"
        await update.message.reply_text(msg)
    except PyMongoError as e:
        logger.error(f"Failed to fetch teams: {e}")
        await update.message.reply_text("Error fetching teams.")
    except TelegramError as e:
        logger.error(f"Failed to send check message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def rankings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user rankings."""
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        scores = {}
        for user in collections[db_name]["user_teams"].find():
            total = 0
            for m, players in user.get("teams", {}).items():
                for i, p in enumerate(players):
                    pt_doc = collections[db_name]["points"].find_one({"player": p}) or {"points": 0}
                    pt = pt_doc["points"]
                    total += pt * (2 if i == 0 else 1.5 if i == 1 else 1)
            scores[user["user_id"]] = total
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        msg = f"Rankings (DB: {db_name}):\n"
        if not sorted_scores:
            msg += "No rankings available."
        for i, (uid, pts) in enumerate(sorted_scores, 1):
            msg += f"{i}. User {uid} - {int(pts)} pts\n"
        await update.message.reply_text(msg)
    except PyMongoError as e:
        logger.error(f"Failed to compute rankings: {e}")
        await update.message.reply_text("Error computing rankings.")
    except TelegramError as e:
        logger.error(f"Failed to send rankings message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def yonrankings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display top 10 users for Yes/No answers."""
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        scores = {}
        for user in collections[db_name]["yon_user_answers"].find():
            total = 0
            for qid, answer in user.get("answers", {}).items():
                correct = collections[db_name]["yon_correct_answers"].find_one({"qid": qid}) or {"answer": ""}
                if correct and answer.lower() == correct["answer"].lower():
                    total += 1
            scores[user["user_id"]] = total
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
        msg = f"Yes/No Rankings (Top 10, DB: {db_name}):\n"
        if not sorted_scores:
            msg += "No rankings available."
        for i, (uid, pts) in enumerate(sorted_scores, 1):
            msg += f"{i}. User {uid} - {pts} correct answers\n"
        await update.message.reply_text(msg)
    except PyMongoError as e:
        logger.error(f"Failed to compute Yes/No rankings: {e}")
        await update.message.reply_text("Error computing rankings.")
    except TelegramError as e:
        logger.error(f"Failed to send yonrankings message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def yon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display Yes/No questions."""
    user_id = str(update.effective_user.id)
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        question = collections[db_name]["yon_questions"].find_one()
        if not question:
            await update.message.reply_text(f"No Yes/No questions available in DB '{db_name}'.")
            return
        await display_yon_question(update, context, user_id, question["qid"], db_name)
    except PyMongoError as e:
        logger.error(f"Failed to fetch Yes/No questions: {e}")
        await update.message.reply_text("Error fetching questions.")
    except TelegramError as e:
        logger.error(f"Failed to send yon message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def edit_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Edit team for a match."""
    user_id = str(update.effective_user.id)
    try:
        db_name = DEFAULT_DB
        match_name = None
        if context.args:
            if len(context.args) == 1:
                match_name = sanitize_input(context.args[0])
            else:
                db_name = sanitize_input(context.args[0])
                match_name = sanitize_input(context.args[1])
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not match_name:
            matches = collections[db_name]["matches"].find()
            if not collections[db_name]["matches"].count_documents({}):
                await update.message.reply_text(f"No matches available in DB '{db_name}'.")
                return
            keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"editteam::{db_name}::{m['name']}")] for m in matches]
            await update.message.reply_text(f"Select a match to edit (DB: {db_name}):", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if not collections[db_name]["matches"].find_one({"name": match_name}):
            await update.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
            return
        if locked_matches[db_name].get(match_name, False):
            await update.message.reply_text(f"❌ This match is locked in DB '{db_name}'.")
            return
        user_teams = collections[db_name]["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
        current_team = user_teams["teams"].get(match_name, [])
        if not current_team:
            await update.message.reply_text(f"No team selected for this match in DB '{db_name}'.")
            return
        keyboard = [
            [InlineKeyboardButton(p, callback_data=f"removeplayer::{db_name}::{match_name}::{p}")]
            for p in current_team
        ]
        keyboard.append([InlineKeyboardButton("Add Players", callback_data=f"create_{db_name}::{match_name}")])
        keyboard.append([InlineKeyboardButton("Clear Team", callback_data=f"clearteam::{db_name}::{match_name}")])
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"back::{db_name}::{match_name}")])
        await update.message.reply_text(
            f"Edit team for {match_name} (DB: {db_name}):\n\n"
            f"Captain: {current_team[0] if current_team else 'N/A'}\n"
            f"Vice-Captain: {current_team[1] if len(current_team) > 1 else 'N/A'}\n"
            f"Players: {', '.join(current_team) if current_team else 'None'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except PyMongoError as e:
        logger.error(f"Failed to edit team: {e}")
        await update.message.reply_text("Error editing team.")
    except TelegramError as e:
        logger.error(f"Failed to send edit_team message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

# === ADMIN COMMANDS ===
async def admhelp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display admin commands."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    help_text = (
        "Admin Commands\n\n"
        "/admin [db_name] - Open admin panel.\n"
        "/addmatch [db_name] <match_name> - Add match.\n"
        "/addteam [db_name] <match_name> <team_name> - Add team.\n"
        "/addplayer [db_name] <match_name> <team_name> <players> - Add players.\n"
        "/points [db_name] <player> <points> - Assign points.\n"
        "/lockmatch [db_name] <match_name> - Lock match.\n"
        "/clear [db_name] - Clear match data.\n"
        "/yonadd [db_name] <question> <option1> <option2> - Add Yes/No question.\n"
        "/yona [db_name] <question_id> <option_number> - Set correct answer.\n"
        "/yonclear [db_name] - Clear Yes/No data.\n"
        "/announcement <group_id> <message> - Send announcement.\n"
        "/target <user_id> <message> - Send message to user.\n"
        "/team [db_name] - View all teams.\n"
        "/backup [db_name] - Download data."
    )
    try:
        await update.message.reply_text(help_text)
    except TelegramError as e:
        logger.error(f"Failed to send admhelp message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Open admin panel."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        matches = collections[db_name]["matches"].find()
        if not collections[db_name]["matches"].count_documents({}):
            await update.message.reply_text(f"No matches available in DB '{db_name}'.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"admin_match_{db_name}::{m['name']}")] for m in matches]
        await update.message.reply_text(f"Admin Panel - Matches (DB: {db_name}):", reply_markup=InlineKeyboardMarkup(keyboard))
    except PyMongoError as e:
        logger.error(f"Failed to fetch admin matches: {e}")
        await update.message.reply_text("Error fetching matches.")
    except TelegramError as e:
        logger.error(f"Failed to send admin message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def addmatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /addmatch [db_name] <match_name>")
        return
    try:
        db_name = DEFAULT_DB
        match_name = sanitize_input(context.args[0])
        if len(context.args) > 1:
            db_name = sanitize_input(context.args[0])
            match_name = sanitize_input(context.args[1])
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not match_name:
            await update.message.reply_text("Invalid match name.")
            return
        if collections[db_name]["matches"].find_one({"name": match_name}):
            await update.message.reply_text(f"Match '{match_name}' already exists in DB '{db_name}'.")
        else:
            collections[db_name]["matches"].insert_one({"name": match_name, "teams": {}, "players": []})
            await update.message.reply_text(f"Match '{match_name}' added to DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to add match: {e}")
        await update.message.reply_text("Error adding match.")
    except TelegramError as e:
        logger.error(f"Failed to send addmatch message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def addteam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a team to a match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addteam [db_name] <match_name> <team_name>")
        return
    try:
        db_name = DEFAULT_DB
        match_name = sanitize_input(context.args[0])
        team_name = sanitize_input(context.args[1])
        if len(context.args) > 2:
            db_name = sanitize_input(context.args[0])
            match_name = sanitize_input(context.args[1])
            team_name = sanitize_input(context.args[2])
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not match_name or not team_name:
            await update.message.reply_text("Invalid match or team name.")
            return
        match_doc = collections[db_name]["matches"].find_one({"name": match_name})
        if not match_doc:
            await update.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
            return
        collections[db_name]["matches"].update_one(
            {"name": match_name},
            {"$set": {f"teams.{team_name}": []}}
        )
        await update.message.reply_text(f"Team '{team_name}' added to '{match_name}' in DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to add team: {e}")
        await update.message.reply_text("Error adding team.")
    except TelegramError as e:
        logger.error(f"Failed to send addteam message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def addplayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add players to a team."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addplayer [db_name] <match_name> <team_name> <player1,player2,...>")
        return
    try:
        db_name = DEFAULT_DB
        match_name = sanitize_input(context.args[0])
        team_name = sanitize_input(context.args[1])
        player_str = " ".join(context.args[2:])
        if len(context.args) > 3:
            db_name = sanitize_input(context.args[0])
            match_name = sanitize_input(context.args[1])
            team_name = sanitize_input(context.args[2])
            player_str = " ".join(context.args[3:])
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        match_doc = collections[db_name]["matches"].find_one({"name": match_name})
        if not match_doc:
            await update.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
            return
        if team_name not in match_doc.get("teams", {}):
            await update.message.reply_text(f"Team '{team_name}' not found in match '{match_name}' in DB '{db_name}'.")
            return
        players = [sanitize_input(p.strip().strip("(),")) for p in player_str.split(",") if p.strip()]
        if not players:
            await update.message.reply_text("No valid players provided.")
            return
        collections[db_name]["matches"].update_one(
            {"name": match_name},
            {
                "$push": {
                    f"teams.{team_name}": {"$each": players},
                    "players": {"$each": players}
                }
            }
        )
        await update.message.reply_text(f"Players added to '{team_name}' in '{match_name}' (DB: {db_name}): {', '.join(players)}")
    except PyMongoError as e:
        logger.error(f"Failed to add players: {e}")
        await update.message.reply_text("Error adding players.")
    except TelegramError as e:
        logger.error(f"Failed to send addplayer message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Assign points to a player."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /points [db_name] <player> <points>")
        return
    try:
        db_name = DEFAULT_DB
        player_name = sanitize_input(context.args[0])
        points_value = context.args[1]
        if len(context.args) > 2:
            db_name = sanitize_input(context.args[0])
            player_name = sanitize_input(context.args[1])
            points_value = context.args[2]
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not player_name:
            await update.message.reply_text("Invalid player name.")
            return
        try:
            pts = int(points_value)
            if pts < 0:
                await update.message.reply_text("Points must be non-negative.")
                return
        except ValueError:
            await update.message.reply_text("Points must be a number.")
            return
        collections[db_name]["points"].update_one(
            {"player": player_name},
            {"$set": {"points": pts}},
            upsert=True
        )
        await update.message.reply_text(f"'{player_name}' assigned {pts} points in DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to assign points: {e}")
        await update.message.reply_text("Error assigning points.")
    except TelegramError as e:
        logger.error(f"Failed to send points message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompt admin to select a match to clear its data."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        matches = collections[db_name]["matches"].find()
        if not collections[db_name]["matches"].count_documents({}):
            await update.message.reply_text(f"No matches available to clear in DB '{db_name}'.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"clear_match_{db_name}::{m['name']}")] for m in matches]
        await update.message.reply_text(f"Select a match to clear its data (DB: {db_name}):", reply_markup=InlineKeyboardMarkup(keyboard))
    except PyMongoError as e:
        logger.error(f"Failed to fetch matches for clear: {e}")
        await update.message.reply_text("Error fetching matches.")
    except TelegramError as e:
        logger.error(f"Failed to send clear message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def lock_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lock a match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /lockmatch [db_name] <match_name>")
        return
    try:
        db_name = DEFAULT_DB
        match_name = sanitize_input(context.args[0])
        if len(context.args) > 1:
            db_name = sanitize_input(context.args[0])
            match_name = sanitize_input(context.args[1])
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        if not match_name or not collections[db_name]["matches"].find_one({"name": match_name}):
            await update.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
            return
        locked_matches[db_name][match_name] = True
        collections[db_name]["locked_matches"].update_one(
            {"match_name": match_name},
            {"$set": {"locked": True}},
            upsert=True
        )
        await update.message.reply_text(f"✅ Match '{match_name}' locked in DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to lock match: {e}")
        await update.message.reply_text("Error locking match.")
    except TelegramError as e:
        logger.error(f"Failed to send lock_match message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def yonadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a Yes/No question."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            'Usage: /yonadd [db_name] "<question>" "<option1>" "<option2>"\n'
            'Example: /yonadd main "Will it rain?" "Yes" "No"'
        )
        return
    try:
        db_name = DEFAULT_DB
        args_start = 0
        if len(context.args) > 3 and context.args[0] in MONGO_URIS:
            db_name = sanitize_input(context.args[0])
            args_start = 1
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        args = shlex.split(" ".join(context.args[args_start:]))
        if len(args) < 3:
            await update.message.reply_text(
                "Provide question and two options.\n"
                'Example: /yonadd [db_name] "Will it rain?" "Yes" "No"'
            )
            return
        question, option1, option2 = args[0], args[-2], args[-1]
        if not question or not option1 or not option2:
            await update.message.reply_text("Question/options cannot be empty.")
            return
        question_id = str(collections[db_name]["yon_questions"].count_documents({}) + 1)
        collections[db_name]["yon_questions"].insert_one({
            "qid": question_id,
            "question": question,
            "options": [option1, option2],
            "options_lower": [option1.lower(), option2.lower()]
        })
        await update.message.reply_text(f"Question {question_id} added to DB '{db_name}': {question} ({option1}/{option2})")
    except PyMongoError as e:
        logger.error(f"Failed to add Yes/No question: {e}")
        await update.message.reply_text("Error adding question.")
    except TelegramError as e:
        logger.error(f"Failed to send yonadd message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def yona(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set correct answer for Yes/No question."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /yona [db_name] <question_id> <option_number>")
        return
    try:
        db_name = DEFAULT_DB
        qid = context.args[0]
        option_num = context.args[1]
        if len(context.args) > 2:
            db_name = sanitize_input(context.args[0])
            qid = context.args[1]
            option_num = context.args[2]
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        try:
            option_num = int(option_num)
            if option_num not in [1, 2]:
                await update.message.reply_text("Option number must be 1 or 2.")
                return
        except ValueError:
            await update.message.reply_text("Option number must be a number.")
            return
        question = collections[db_name]["yon_questions"].find_one({"qid": qid})
        if not question:
            await update.message.reply_text(f"Question '{qid}' not found in DB '{db_name}'.")
            return
        answer = question["options"][option_num - 1]
        collections[db_name]["yon_correct_answers"].update_one(
            {"qid": qid},
            {"$set": {"answer": answer}},
            upsert=True
        )
        await update.message.reply_text(f"Correct answer for Q{qid} set in DB '{db_name}': {answer}")
    except PyMongoError as e:
        logger.error(f"Failed to set Yes/No answer: {e}")
        await update.message.reply_text("Error setting answer.")
    except TelegramError as e:
        logger.error(f"Failed to send yona message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def yonclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear Yes/No questions and answers."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        collections[db_name]["yon_questions"].delete_many({})
        collections[db_name]["yon_user_answers"].delete_many({})
        collections[db_name]["yon_correct_answers"].delete_many({})
        await update.message.reply_text(f"Yes/No data cleared in DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to clear Yes/No data: {e}")
        await update.message.reply_text("Error clearing data.")
    except TelegramError as e:
        logger.error(f"Failed to send yonclear message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def announcement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send announcement to a group."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /announcement <group_id> <message>")
        return
    try:
        group_id, message = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=group_id, text=message)
        await update.message.reply_text("Announcement sent.")
    except TelegramError as e:
        logger.error(f"Failed to send announcement: {e}")
        await update.message.reply_text(f"Failed to send announcement: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in announcement: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send message to a user."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /target <user_id> <message>")
        return
    try:
        user_id, message = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=user_id, text=message)
        await update.message.reply_text("Message sent.")
    except TelegramError as e:
        logger.error(f"Failed to send target message: {e}")
        await update.message.reply_text(f"Failed to send message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in target: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View all teams."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        teams = collections[db_name]["user_teams"].find()
        msg = f"All Teams (DB: {db_name}):\n\n"
        for user in teams:
            msg += f"User {user['user_id']}:\n"
            for match, players in user.get("teams", {}).items():
                msg += f"{match}: {', '.join(players)}\n"
            msg += "\n"
        await update.message.reply_text(msg or f"No teams found in DB '{db_name}'.")
    except PyMongoError as e:
        logger.error(f"Failed to fetch teams: {e}")
        await update.message.reply_text("Error fetching teams.")
    except TelegramError as e:
        logger.error(f"Failed to send team message: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download data as JSON."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized.")
        return
    try:
        db_name = sanitize_input(context.args[0]) if context.args else DEFAULT_DB
        if db_name not in MONGO_URIS:
            await update.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
            return
        data = {}
        for name, collection in collections[db_name].items():
            data[name] = list(collection.find({}, {"_id": 0}))
        backup_file = f"backup_{db_name}.json"
        with open(backup_file, "w") as f:
            json.dump(data, f)
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=open(backup_file, "rb"),
            filename=backup_file
        )
    except (PyMongoError, IOError) as e:
        logger.error(f"Failed to create backup: {e}")
        await update.message.reply_text("Error creating backup.")
    except TelegramError as e:
        logger.error(f"Failed to send backup: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

# === HELPER FUNCTIONS ===
async def display_yon_question(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: str, question_id: str, db_name: str):
    """Display a Yes/No question."""
    try:
        question = collections[db_name]["yon_questions"].find_one({"qid": question_id})
        if not question:
            await update.message.reply_text(f"Question '{question_id}' not found in DB '{db_name}'.")
            return
        options = question["options"]
        keyboard = [
            [
                InlineKeyboardButton(options[0], callback_data=f"yon_answer::{db_name}::{question_id}::0"),
                InlineKeyboardButton(options[1], callback_data=f"yon_answer::{db_name}::{question_id}::1")
            ]
        ]
        question_ids = [q["qid"] for q in collections[db_name]["yon_questions"].find().sort("qid")]
        nav_buttons = []
        current_idx = question_ids.index(question_id)
        if current_idx > 0:
            nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"yon_nav::{db_name}::{question_ids[current_idx-1]}"))
        if current_idx < len(question_ids) - 1:
            nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"yon_nav::{db_name}::{question_ids[current_idx+1]}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
        await update.message.reply_text(
            f"Question {question_id} (DB: {db_name}): {question['question']}\nChoose an option:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except PyMongoError as e:
        logger.error(f"Failed to display Yes/No question: {e}")
        await update.message.reply_text("Error displaying question.")
    except TelegramError as e:
        logger.error(f"Failed to send yon question: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

# === CALLBACK HANDLER ===
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button clicks."""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)
    try:
        if data.startswith("user_match_"):
            try:
                db_name, match_name = map(sanitize_input, data[len("user_match_"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            if not collections[db_name]["matches"].find_one({"name": match_name}):
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            keyboard = [
                [InlineKeyboardButton("Create/Edit Team", callback_data=f"create_{db_name}::{match_name}")],
                [InlineKeyboardButton("Add Bet Amount", callback_data=f"addamount::{db_name}::{match_name}")]
            ]
            await query.message.reply_text(
                f"Selected match: {match_name} (DB: {db_name})\nChoose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        elif data.startswith("addamount::"):
            try:
                db_name, match_name = map(sanitize_input, data[len("addamount::"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            if not collections[db_name]["matches"].find_one({"name": match_name}):
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            if locked_matches[db_name].get(match_name, False):
                await query.message.reply_text(f"❌ This match is locked in DB '{db_name}'.")
                return
            await query.message.reply_text(f"Enter bet amount: /addamount {db_name} {match_name} <amount>")

        elif data.startswith("create_"):
            try:
                db_name, match_name = map(sanitize_input, data[len("create_"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            if not collections[db_name]["matches"].find_one({"name": match_name}):
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            if locked_matches[db_name].get(match_name, False):
                await query.message.reply_text(f"❌ This match is locked in DB '{db_name}'.")
                return
            match = collections[db_name]["matches"].find_one({"name": match_name}) or {"players": []}
            players = match["players"]
            if not players:
                await query.message.reply_text(f"No players available for this match in DB '{db_name}'.")
                return
            keyboard = [[InlineKeyboardButton(p, callback_data=f"addplayer::{db_name}::{match_name}::{p}")] for p in players]
            await query.message.reply_text(
                f"Select players for {match_name} (DB: {db_name}) (max 11, first Captain, second Vice-Captain):",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        elif data.startswith("addplayer::"):
            try:
                db_name, match_name, player = data[len("addplayer::"):].split("::")
                db_name = sanitize_input(db_name)
                match_name = sanitize_input(match_name)
                player = sanitize_input(player)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            if not collections[db_name]["matches"].find_one({"name": match_name}):
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            if locked_matches[db_name].get(match_name, False):
                await query.message.reply_text(f"❌ This match is locked in DB '{db_name}'.")
                return
            user_teams = collections[db_name]["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
            current_team = user_teams["teams"].get(match_name, [])
            if player in current_team:
                await query.message.reply_text(f"'{player}' already in team for '{match_name}' in DB '{db_name}'.")
                return
            if len(current_team) >= 11:
                await query.message.reply_text("Team full (11 players).")
                return
            current_team.append(player)
            collections[db_name]["user_teams"].update_one(
                {"user_id": user_id},
                {"$set": {f"teams.{match_name}": current_team}},
                upsert=True
            )
            await query.message.reply_text(f"'{player}' added to '{match_name}' team in DB '{db_name}'.")

        elif data.startswith("removeplayer::"):
            try:
                db_name, match_name, player = data[len("removeplayer::"):].split("::")
                db_name = sanitize_input(db_name)
                match_name = sanitize_input(match_name)
                player = sanitize_input(player)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            user_teams = collections[db_name]["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
            current_team = user_teams["teams"].get(match_name, [])
            if player not in current_team:
                await query.message.reply_text(f"'{player}' not in team for '{match_name}' in DB '{db_name}'.")
                return
            current_team.remove(player)
            collections[db_name]["user_teams"].update_one(
                {"user_id": user_id},
                {"$set": {f"teams.{match_name}": current_team}}
            )
            await query.message.reply_text(f"'{player}' removed from '{match_name}' team in DB '{db_name}'.")

        elif data.startswith("clearteam::"):
            try:
                db_name, match_name = map(sanitize_input, data[len("clearteam::"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            if not collections[db_name]["matches"].find_one({"name": match_name}):
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            collections[db_name]["user_teams"].update_one(
                {"user_id": user_id},
                {"$unset": {f"teams.{match_name}": ""}}
            )
            await query.message.reply_text(f"Team for '{match_name}' cleared in DB '{db_name}'.")

        elif data.startswith("back::"):
            try:
                db_name, match_name = map(sanitize_input, data[len("back::"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            context.args = [db_name, match_name]
            await edit_team(update, context)

        elif data.startswith("yon_answer::"):
            try:
                db_name, qid, option_idx = data[len("yon_answer::"):].split("::")
                db_name = sanitize_input(db_name)
                option_idx = int(option_idx)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            question = collections[db_name]["yon_questions"].find_one({"qid": qid})
            if not question or option_idx not in [0, 1]:
                await query.message.reply_text(f"Question '{qid}' not found or invalid option in DB '{db_name}'.")
                return
            answer = question["options"][option_idx]
            collections[db_name]["yon_user_answers"].update_one(
                {"user_id": user_id},
                {"$set": {f"answers.{qid}": answer}},
                upsert=True
            )
            await query.message.reply_text(f"Answer recorded: {answer} in DB '{db_name}'.")

        elif data.startswith("yon_nav::"):
            try:
                db_name, next_qid = data[len("yon_nav::"):].split("::")
                db_name = sanitize_input(db_name)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            await display_yon_question(query, context, user_id, next_qid, db_name)

        elif data.startswith("clear_match_"):
            if not is_admin(query.from_user.id):
                await query.message.reply_text("❌ Unauthorized.")
                return
            try:
                db_name, match_name = map(sanitize_input, data[len("clear_match_"):].split("::"))
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if db_name not in MONGO_URIS:
                await query.message.reply_text(f"Database '{db_name}' not found. Available: {', '.join(MONGO_URIS.keys())}")
                return
            match_doc = collections[db_name]["matches"].find_one({"name": match_name})
            if not match_doc:
                await query.message.reply_text(f"Match '{match_name}' not found in DB '{db_name}'.")
                return
            players = match_doc.get("players", [])
            collections[db_name]["matches"].delete_one({"name": match_name})
            if players:
                collections[db_name]["points"].delete_many({"player": {"$in": players}})
            collections[db_name]["locked_matches"].delete_one({"match_name": match_name})
            if match_name in locked_matches[db_name]:
                del locked_matches[db_name][match_name]
            await query.message.reply_text(f"All data for match '{match_name}' cleared in DB '{db_name}'. User teams and bets preserved.")

    except PyMongoError as e:
        logger.error(f"Callback error: {e}")
        await query.message.reply_text("Error processing request.")
    except TelegramError as e:
        logger.error(f"Failed to send callback response: {e}")
        await query.message.reply_text("An error occurred. Please try again.")
    except Exception as e:
        logger.error(f"Unexpected callback error: {e}")
        await query.message.reply_text("An unexpected error occurred.")

# === MAIN ===
def main():
    """Run the bot."""
    try:
        application = Application.builder().token(BOT_TOKEN).build()

        # Schedule storage check job
        job_queue = application.job_queue
        job_queue.run_repeating(check_storage, interval=1800, first=10)  # Check every 30 minutes, start after 10 seconds

        # User commands
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help))
        application.add_handler(CommandHandler("schedule", schedule))
        application.add_handler(CommandHandler("addamount", addamount))
        application.add_handler(CommandHandler("profile", profile))
        application.add_handler(CommandHandler("check", check))
        application.add_handler(CommandHandler("rankings", rankings))
        application.add_handler(CommandHandler("yon", yon))
        application.add_handler(CommandHandler("yonrankings", yonrankings))
        application.add_handler(CommandHandler("editteam", edit_team))

        # Admin commands
        application.add_handler(CommandHandler("admhelp", admhelp))
        application.add_handler(CommandHandler("admin", admin))
        application.add_handler(CommandHandler("addmatch", addmatch))
        application.add_handler(CommandHandler("addteam", addteam))
        application.add_handler(CommandHandler("addplayer", addplayer))
        application.add_handler(CommandHandler("points", points))
        application.add_handler(CommandHandler("clear", clear))
        application.add_handler(CommandHandler("lockmatch", lock_match))
        application.add_handler(CommandHandler("yonadd", yonadd))
        application.add_handler(CommandHandler("yona", yona))
        application.add_handler(CommandHandler("yonclear", yonclear))
        application.add_handler(CommandHandler("announcement", announcement))
        application.add_handler(CommandHandler("target", target))
        application.add_handler(CommandHandler("team", team))
        application.add_handler(CommandHandler("backup", backup))

        # Callback handler
        application.add_handler(CallbackQueryHandler(button_callback))

        application.run_polling()
    finally:
        for client in mongo_clients.values():
            client.close()  # Close all MongoDB connections

if __name__ == "__main__":
    main()
