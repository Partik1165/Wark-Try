import logging
import json
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.error import TelegramError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv
import os
import shlex

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB setup
try:
    client = MongoClient(MONGO_URI, maxPoolSize=50)
    client.admin.command('ping')  # Test connection
    db = client["cricket_bot"]
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

collections = {
    "matches": db["matches"],
    "user_teams": db["user_teams"],
    "points": db["points"],
    "amounts": db["amounts"],
    "yon_questions": db["yon_questions"],
    "yon_user_answers": db["yon_user_answers"],
    "yon_correct_answers": db["yon_correct_answers"],
    "locked_matches": db["locked_matches"]
}

# Create indexes for performance
collections["matches"].create_index("name", unique=True)
collections["user_teams"].create_index("user_id")
collections["yon_questions"].create_index("qid")
collections["locked_matches"].create_index("match_name")

# In-memory cache for locked matches
locked_matches = {doc["match_name"]: doc["locked"] for doc in collections["locked_matches"].find()}

def is_admin(user_id):
    """Check if the user is an admin."""
    return user_id in ADMIN_IDS

def sanitize_input(text):
    """Remove potentially dangerous characters from input."""
    return re.sub(r'[^\w\s,.]', '', text.strip())

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
        "/schedule - View matches.\n"
        "/editteam [match_name] - Edit team.\n"
        "/addamount <match_name> <amount> - Set bet amount.\n"
        "/check - View teams.\n"
        "/profile - View teams and bets.\n"
        "/rankings - See rankings.\n"
        "/yon - Answer Yes/No questions.\n"
        "/yonrankings - View Yes/No rankings.\n"
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
        matches = collections["matches"].find()
        if not collections["matches"].count_documents({}):
            await update.message.reply_text("No matches available.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"user_match_{m['name']}")] for m in matches]
        await update.message.reply_text("Select a match:", reply_markup=InlineKeyboardMarkup(keyboard))
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
            matches = collections["matches"].find()
            if not collections["matches"].count_documents({}):
                await update.message.reply_text("No matches available.")
                return
            keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"addamount::{m['name']}")] for m in matches]
            await update.message.reply_text("Select a match to set bet:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        match_name = sanitize_input(context.args[0])
        if not match_name or not collections["matches"].find_one({"name": match_name}):
            await update.message.reply_text("Match not found.")
            return
        if locked_matches.get(match_name, False):
            await update.message.reply_text("❌ This match is locked.")
            return
        try:
            amount = int(context.args[1])
            if amount <= 0:
                await update.message.reply_text("Enter a positive amount.")
                return
        except ValueError:
            await update.message.reply_text("Invalid amount (must be a number).")
            return
        collections["amounts"].update_one(
            {"user_id": user_id},
            {"$set": {f"bets.{match_name}": amount}},
            upsert=True
        )
        await update.message.reply_text(
            f"Bet of {amount} points added for {match_name}. Tag @Trainer_OFFicial in the group."
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
    msg = f"📋 *Your Profile* 📋\n\n"
    try:
        user_teams = collections["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
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
        
        user_amounts = collections["amounts"].find_one({"user_id": user_id}) or {"bets": {}}
        if not user_amounts["bets"]:
            msg += "No bets placed.\n"
        else:
            msg += "Your Bets:\n"
            for match, amount in user_amounts["bets"].items():
                msg += f"{match}: {amount} points\n"
        
        user_answers = collections["yon_user_answers"].find_one({"user_id": user_id}) or {"answers": {}}
        if not user_answers["answers"]:
            msg += "No Yes/No answers.\n"
        else:
            msg += "Your Yes/No Answers:\n"
            for qid, answer in user_answers["answers"].items():
                question = collections["yon_questions"].find_one({"qid": qid}) or {"question": "Unknown"}
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
        user_teams = collections["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
        if not user_teams["teams"]:
            await update.message.reply_text("No teams selected.")
            return
        msg = "Your teams:\n\n"
        for match, players in user_teams["teams"].items():
            msg += f"{match}:\n"
            for i, p in enumerate(players):
                role = " (Captain)" if i == 0 else " (Vice-Captain)" if i == 1 else ""
                msg += f"- {p}{role}\n"
            user_amounts = collections["amounts"].find_one({"user_id": user_id}) or {"bets": {}}
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
        scores = {}
        for user in collections["user_teams"].find():
            total = 0
            for m, players in user.get("teams", {}).items():
                for i, p in enumerate(players):
                    pt_doc = collections["points"].find_one({"player": p}) or {"points": 0}
                    pt = pt_doc["points"]
                    total += pt * (2 if i == 0 else 1.5 if i == 1 else 1)
            scores[user["user_id"]] = total
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        msg = "Rankings:\n"
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
        scores = {}
        for user in collections["yon_user_answers"].find():
            total = 0
            for qid, answer in user.get("answers", {}).items():
                correct = collections["yon_correct_answers"].find_one({"qid": qid}) or {"answer": ""}
                if correct and answer.lower() == correct["answer"].lower():
                    total += 1
            scores[user["user_id"]] = total
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
        msg = "Yes/No Rankings (Top 10):\n"
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
        question = collections["yon_questions"].find_one()
        if not question:
            await update.message.reply_text("No Yes/No questions available.")
            return
        await display_yon_question(update, context, user_id, question["qid"])
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
        if not context.args:
            matches = collections["matches"].find()
            if not collections["matches"].count_documents({}):
                await update.message.reply_text("No matches available.")
                return
            keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"editteam::{m['name']}")] for m in matches]
            await update.message.reply_text("Select a match to edit:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        match_name = sanitize_input(context.args[0])
        if not match_name or not collections["matches"].find_one({"name": match_name}):
            await update.message.reply_text("Match not found.")
            return
        if locked_matches.get(match_name, False):
            await update.message.reply_text("❌ This match is locked.")
            return
        user_teams = collections["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
        current_team = user_teams["teams"].get(match_name, [])
        if not current_team:
            await update.message.reply_text("No team selected for this match.")
            return
        keyboard = [
            [InlineKeyboardButton(p, callback_data=f"removeplayer::{match_name}::{p}")]
            for p in current_team
        ]
        keyboard.append([InlineKeyboardButton("Add Players", callback_data=f"create_{match_name}")])
        keyboard.append([InlineKeyboardButton("Clear Team", callback_data=f"clearteam::{match_name}")])
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"back::{match_name}")])
        await update.message.reply_text(
            f"Edit team for {match_name}:\n\n"
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
        "/admin - Open admin panel.\n"
        "/addmatch <match_name> - Add match.\n"
        "/addteam <match_name> <team_name> - Add team.\n"
        "/addplayer <match_name> <team_name> <players> - Add players.\n"
        "/points <player> <points> - Assign points.\n"
        "/lockmatch <match_name> - Lock match.\n"
        "/clear - Clear match data.\n"
        "/yonadd <question> <option1> <option2> - Add Yes/No question.\n"
        "/yona <question_id> <option_number> - Set correct answer.\n"
        "/yonclear - Clear Yes/No data.\n"
        "/announcement <group_id> <message> - Send announcement.\n"
        "/target <user_id> <message> - Send message to user.\n"
        "/team - View all teams.\n"
        "/backup - Download data."
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
        matches = collections["matches"].find()
        if not collections["matches"].count_documents({}):
            await update.message.reply_text("No matches available.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"admin_match_{m['name']}")] for m in matches]
        await update.message.reply_text("Admin Panel - Matches:", reply_markup=InlineKeyboardMarkup(keyboard))
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
        await update.message.reply_text("Usage: /addmatch <match_name>")
        return
    try:
        match = sanitize_input(context.args[0])
        if not match:
            await update.message.reply_text("Invalid match name.")
            return
        if collections["matches"].find_one({"name": match}):
            await update.message.reply_text("Match exists.")
        else:
            collections["matches"].insert_one({"name": match, "teams": {}, "players": []})
            await update.message.reply_text(f"Match {match} added.")
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
        await update.message.reply_text("Usage: /addteam <match_name> <team_name>")
        return
    try:
        match, team = sanitize_input(context.args[0]), sanitize_input(context.args[1])
        if not match or not team:
            await update.message.reply_text("Invalid match or team name.")
            return
        if not collections["matches"].find_one({"name": match}):
            await update.message.reply_text("Match not found.")
            return
        collections["matches"].update_one(
            {"name": match},
            {"$set": {f"teams.{team}": []}}
        )
        await update.message.reply_text(f"Team {team} added to {match}.")
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
        await update.message.reply_text("Usage: /addplayer <match_name> <team_name> <player1,player2,...>")
        return
    try:
        match, team = sanitize_input(context.args[0]), sanitize_input(context.args[1])
        match_doc = collections["matches"].find_one({"name": match})
        if not match_doc:
            await update.message.reply_text("Match not found.")
            return
        if team not in match_doc.get("teams", {}):
            await update.message.reply_text("Team not found in match.")
            return
        player_str = " ".join(context.args[2:])
        players = [sanitize_input(p.strip().strip("(),")) for p in player_str.split(",") if p.strip()]
        if not players:
            await update.message.reply_text("No valid players provided.")
            return
        collections["matches"].update_one(
            {"name": match},
            {
                "$push": {
                    f"teams.{team}": {"$each": players},
                    "players": {"$each": players}
                }
            }
        )
        await update.message.reply_text(f"Players added to {team} in {match}: {', '.join(players)}")
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
        await update.message.reply_text("Usage: /points <player> <points>")
        return
    try:
        player, pts = sanitize_input(context.args[0]), context.args[1]
        if not player:
            await update.message.reply_text("Invalid player name.")
            return
        try:
            pts = int(pts)
            if pts < 0:
                await update.message.reply_text("Points must be non-negative.")
                return
        except ValueError:
            await update.message.reply_text("Points must be a number.")
            return
        collections["points"].update_one(
            {"player": player},
            {"$set": {"points": pts}},
            upsert=True
        )
        await update.message.reply_text(f"{player} got {pts} points.")
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
        matches = collections["matches"].find()
        if not collections["matches"].count_documents({}):
            await update.message.reply_text("No matches available to clear.")
            return
        keyboard = [[InlineKeyboardButton(m["name"], callback_data=f"clear_match_{m['name']}")] for m in matches]
        await update.message.reply_text("Select a match to clear its data:", reply_markup=InlineKeyboardMarkup(keyboard))
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
        await update.message.reply_text("Usage: /lockmatch <match_name>")
        return
    try:
        match_name = sanitize_input(context.args[0])
        if not match_name or not collections["matches"].find_one({"name": match_name}):
            await update.message.reply_text("Match not found.")
            return
        locked_matches[match_name] = True
        collections["locked_matches"].update_one(
            {"match_name": match_name},
            {"$set": {"locked": True}},
            upsert=True
        )
        await update.message.reply_text(f"✅ Match '{match_name}' locked.")
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
            'Usage: /yonadd "<question>" "<option1>" "<option2>"\n'
            'Example: /yonadd "Will it rain?" "Yes" "No"'
        )
        return
    try:
        args = shlex.split(" ".join(context.args))
        if len(args) < 3:
            await update.message.reply_text(
                "Provide question and two options.\n"
                'Example: /yonadd "Will it rain?" "Yes" "No"'
            )
            return
        question, option1, option2 = args[0], args[-2], args[-1]
        if not question or not option1 or not option2:
            await update.message.reply_text("Question/options cannot be empty.")
            return
        question_id = str(collections["yon_questions"].count_documents({}) + 1)
        collections["yon Questions"].insert_one({
            "qid": question_id,
            "question": question,
            "options": [option1, option2],
            "options_lower": [option1.lower(), option2.lower()]
        })
        await update.message.reply_text(f"Question {question_id} added: {question} ({option1}/{option2})")
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
        await update.message.reply_text("Usage: /yona <question_id> <option_number>")
        return
    try:
        qid, option_num = context.args[0], context.args[1]
        try:
            option_num = int(option_num)
            if option_num not in [1, 2]:
                await update.message.reply_text("Option number must be 1 or 2.")
                return
        except ValueError:
            await update.message.reply_text("Option number must be a number.")
            return
        question = collections["yon_questions"].find_one({"qid": qid})
        if not question:
            await update.message.reply_text("Question not found.")
            return
        answer = question["options"][option_num - 1]
        collections["yon_correct_answers"].update_one(
            {"qid": qid},
            {"$set": {"answer": answer}},
            upsert=True
        )
        await update.message.reply_text(f"Correct answer for Q{qid} set: {answer}")
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
        collections["yon_questions"].delete_many({})
        collections["yon_user_answers"].delete_many({})
        collections["yon_correct_answers"].delete_many({})
        await update.message.reply_text("Yes/No data cleared.")
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
        teams = collections["user_teams"].find()
        msg = "All Teams:\n\n"
        for user in teams:
            msg += f"User {user['user_id']}:\n"
            for match, players in user.get("teams", {}).items():
                msg += f"{match}: {', '.join(players)}\n"
            msg += "\n"
        await update.message.reply_text(msg or "No teams found.")
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
        data = {}
        for name, collection in collections.items():
            data[name] = list(collection.find({}, {"_id": 0}))
        with open("backup.json", "w") as f:
            json.dump(data, f)
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=open("backup.json", "rb"),
            filename="backup.json"
        )
    except (PyMongoError, IOError) as e:
        logger.error(f"Failed to create backup: {e}")
        await update.message.reply_text("Error creating backup.")
    except TelegramError as e:
        logger.error(f"Failed to send backup: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

# === HELPER FUNCTIONS ===
async def display_yon_question(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: str, question_id: str):
    """Display a Yes/No question."""
    try:
        question = collections["yon_questions"].find_one({"qid": question_id})
        if not question:
            await update.message.reply_text("Question not found.")
            return
        options = question["options"]
        keyboard = [
            [
                InlineKeyboardButton(options[0], callback_data=f"yon_answer::{question_id}::0"),
                InlineKeyboardButton(options[1], callback_data=f"yon_answer::{question_id}::1")
            ]
        ]
        question_ids = [q["qid"] for q in collections["yon_questions"].find().sort("qid")]
        nav_buttons = []
        current_idx = question_ids.index(question_id)
        if current_idx > 0:
            nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"yon_nav::{question_ids[current_idx-1]}"))
        if current_idx < len(question_ids) - 1:
            nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"yon_nav::{question_ids[current_idx+1]}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
        await update.message.reply_text(
            f"Question {question_id}: {question['question']}\nChoose an option:",
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
            match_name = sanitize_input(data[len("user_match_"):])
            if not collections["matches"].find_one({"name": match_name}):
                await query.message.reply_text("Match not found.")
                return
            keyboard = [
                [InlineKeyboardButton("Create/Edit Team", callback_data=f"create_{match_name}")],
                [InlineKeyboardButton("Add Bet Amount", callback_data=f"addamount::{match_name}")]
            ]
            await query.message.reply_text(
                f"Selected match: {match_name}\nChoose an action:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        elif data.startswith("addamount::"):
            match_name = sanitize_input(data[len("addamount::"):])
            if not collections["matches"].find_one({"name": match_name}):
                await query.message.reply_text("Match not found.")
                return
            if locked_matches.get(match_name, False):
                await query.message.reply_text("❌ This match is locked.")
                return
            await query.message.reply_text(f"Enter bet amount: /addamount {match_name} <amount>")

        elif data.startswith("create_"):
            match_name = sanitize_input(data[len("create_"):])
            if not collections["matches"].find_one({"name": match_name}):
                await query.message.reply_text("Match not found.")
                return
            if locked_matches.get(match_name, False):
                await query.message.reply_text("❌ This match is locked.")
                return
            match = collections["matches"].find_one({"name": match_name}) or {"players": []}
            players = match["players"]
            if not players:
                await query.message.reply_text("No players available for this match.")
                return
            keyboard = [[InlineKeyboardButton(p, callback_data=f"addplayer::{match_name}::{p}")] for p in players]
            await query.message.reply_text(
                f"Select players for {match_name} (max 11, first Captain, second Vice-Captain):",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        elif data.startswith("addplayer::"):
            try:
                _, match_name, player = data.split("::")
                match_name = sanitize_input(match_name)
                player = sanitize_input(player)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            if not collections["matches"].find_one({"name": match_name}):
                await query.message.reply_text("Match not found.")
                return
            if locked_matches.get(match_name, False):
                await query.message.reply_text("❌ This match is locked.")
                return
            user_teams = collections["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
            current_team = user_teams["teams"].get(match_name, [])
            if player in current_team:
                await query.message.reply_text(f"{player} already in team.")
                return
            if len(current_team) >= 11:
                await query.message.reply_text("Team full (11 players).")
                return
            current_team.append(player)
            collections["user_teams"].update_one(
                {"user_id": user_id},
                {"$set": {f"teams.{match_name}": current_team}},
                upsert=True
            )
            await query.message.reply_text(f"{player} added to {match_name} team.")

        elif data.startswith("removeplayer::"):
            try:
                _, match_name, player = data.split("::")
                match_name = sanitize_input(match_name)
                player = sanitize_input(player)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            user_teams = collections["user_teams"].find_one({"user_id": user_id}) or {"teams": {}}
            current_team = user_teams["teams"].get(match_name, [])
            if player not in current_team:
                await query.message.reply_text(f"{player} not in team.")
                return
            current_team.remove(player)
            collections["user_teams"].update_one(
                {"user_id": user_id},
                {"$set": {f"teams.{match_name}": current_team}}
            )
            await query.message.reply_text(f"{player} removed from {match_name} team.")

        elif data.startswith("clearteam::"):
            match_name = sanitize_input(data[len("clearteam::"):])
            if not collections["matches"].find_one({"name": match_name}):
                await query.message.reply_text("Match not found.")
                return
            collections["user_teams"].update_one(
                {"user_id": user_id},
                {"$unset": {f"teams.{match_name}": ""}}
            )
            await query.message.reply_text(f"Team for {match_name} cleared.")

        elif data.startswith("back::"):
            match_name = sanitize_input(data[len("back::"):])
            context.args = [match_name]
            await edit_team(update, context)

        elif data.startswith("yon_answer::"):
            try:
                _, qid, option_idx = data.split("::")
                option_idx = int(option_idx)
            except ValueError:
                await query.message.reply_text("Invalid callback data.")
                return
            question = collections["yon_questions"].find_one({"qid": qid})
            if not question or option_idx not in [0, 1]:
                await query.message.reply_text("Question not found or invalid option.")
                return
            answer = question["options"][option_idx]
            collections["yon_user_answers"].update_one(
                {"user_id": user_id},
                {"$set": {f"answers.{qid}": answer}},
                upsert=True
            )
            await query.message.reply_text(f"Answer recorded: {answer}")

        elif data.startswith("yon_nav::"):
            next_qid = data[len("yon_nav::"):]
            await display_yon_question(query, context, user_id, next_qid)

        elif data.startswith("clear_match_"):
            if not is_admin(query.from_user.id):
                await query.message.reply_text("❌ Unauthorized.")
                return
            match_name = sanitize_input(data[len("clear_match_"):])
            match_doc = collections["matches"].find_one({"name": match_name})
            if not match_doc:
                await query.message.reply_text("Match not found.")
                return
            # Collect players to remove their points
            players = match_doc.get("players", [])
            # Delete match from matches collection
            collections["matches"].delete_one({"name": match_name})
            # Delete points for players in this match
            if players:
                collections["points"].delete_many({"player": {"$in": players}})
            # Delete locked status
            collections["locked_matches"].delete_one({"match_name": match_name})
            if match_name in locked_matches:
                del locked_matches[match_name]
            # User teams and amounts are preserved
            await query.message.reply_text(f"All data for match '{match_name}' cleared. User teams and bets preserved.")

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
        client.close()  # Close MongoDB connection

if __name__ == "__main__":
    main()
