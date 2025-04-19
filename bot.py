import logging
import json
import asyncio
import os
import time
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
from telegram.error import TelegramError
from pymongo import MongoClient
from pymongo.errors import ConnectionError

# Load environment variables from .env
load_dotenv()

# Configuration
ADMIN_IDS = [6293126201, 5460768109, 5220416927]
BOT_TOKEN = os.getenv("BOT_TOKEN")
VERIFICATION_GROUP_ID = int(os.getenv("VERIFICATION_GROUP_ID"))

# MongoDB Configuration
MONGO_URIS = [os.getenv(f"MONGO_URI_{i}") for i in range(1, 10) if os.getenv(f"MONGO_URI_{i}")]
MONGO_CHECK_INTERVAL = 3600  # Check every hour (in seconds)
FREE_SPACE_THRESHOLD_MB = 20  # Notify when free space <= 20 MB
COLLECTION_NAME = "cricket_data"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Clients
mongo_clients = []

def init_mongo_clients():
    """Initialize MongoDB connections with retry logic."""
    global mongo_clients
    for uri in MONGO_URIS:
        for attempt in range(3):
            try:
                client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                client.admin.command('ping')
                db_name = uri.split('/')[-1].split('?')[0]
                mongo_clients.append((client, db_name))
                logger.info(f"Connected to MongoDB database: {db_name}")
                # Initialize collection if it doesn't exist
                db = client[db_name]
                if COLLECTION_NAME not in db.list_collection_names():
                    db[COLLECTION_NAME].insert_one({
                        "matches": {},
                        "user_teams": {},
                        "points": {},
                        "amounts": {},
                        "yon_questions": {},
                        "yon_user_answers": {},
                        "yon_correct_answers": {},
                        "pending_bets": {},
                        "captains": {},
                        "vice_captains": {}
                    })
                break
            except ConnectionError:
                logger.warning(f"Connection attempt {attempt + 1} failed for {uri}")
                time.sleep(2)
            except Exception as e:
                logger.error(f"Unexpected error connecting to {uri}: {e}")
                break

async def check_mongo_storage(context: ContextTypes.DEFAULT_TYPE):
    """Check storage for all MongoDB databases and notify admins if free space is low."""
    for client, db_name in mongo_clients:
        try:
            db = client[db_name]
            stats = db.command("dbStats", freeStorage=1)
            free_storage_mb = stats.get('freeStorageSize', 0) / (1024 * 1024)
            total_size_mb = stats.get('totalSize', 0) / (1024 * 1024)
            logger.info(f"Database {db_name}: Free space = {free_storage_mb:.2f} MB, Total size = {total_size_mb:.2f} MB")

            if free_storage_mb <= FREE_SPACE_THRESHOLD_MB:
                message = (
                    f"⚠️ Low Storage Alert ⚠️\n"
                    f"Database: {db_name}\n"
                    f"Free Space: {free_storage_mb:.2f} MB\n"
                    f"Total Size: {total_size_mb:.2f} MB\n"
                    f"Please take action to free up space or expand storage."
                )
                for admin_id in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=message,
                            parse_mode="Markdown"
                        )
                        logger.info(f"Sent low storage notification to admin {admin_id} for {db_name}")
                    except TelegramError as e:
                        logger.error(f"Failed to send notification to admin {admin_id}: {e}")
        except Exception as e:
            logger.error(f"Error checking storage for database {db_name}: {e}")

async def start_storage_monitoring(application: Application):
    """Start periodic MongoDB storage monitoring."""
    application.job_queue.run_repeating(
        callback=check_mongo_storage,
        interval=MONGO_CHECK_INTERVAL,
        first=10
    )
    logger.info("MongoDB storage monitoring started")

async def check_storage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually check MongoDB storage (admin only)."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    await check_mongo_storage(context)
    await update.message.reply_text("Storage check completed. Notifications sent if any database is low on space.")

# MongoDB Data Operations
def load_db(client, db_name):
    """Load data from MongoDB collection."""
    try:
        db = client[db_name]
        data = db[COLLECTION_NAME].find_one()
        if not data:
            data = {
                "matches": {},
                "user_teams": {},
                "points": {},
                "amounts": {},
                "yon_questions": {},
                "yon_user_answers": {},
                "yon_correct_answers": {},
                "pending_bets": {},
                "captains": {},
                "vice_captains": {}
            }
            db[COLLECTION_NAME].insert_one(data)
        data.pop('_id', None)  # Remove MongoDB's _id field
        return data
    except Exception as e:
        logger.error(f"Failed to load data from {db_name}: {e}")
        return None

def save_db(client, db_name, data):
    """Save data to MongoDB collection."""
    try:
        db = client[db_name]
        db[COLLECTION_NAME].update_one({}, {"$set": data}, upsert=True)
    except Exception as e:
        logger.error(f"Failed to save data to {db_name}: {e}")

# In-memory cache for the primary database
db = None
primary_client = None
primary_db_name = None

locked_matches = {}

def is_admin(user_id):
    """Check if the user is an admin."""
    return user_id in ADMIN_IDS

# Initialize MongoDB and load data
init_mongo_clients()
if mongo_clients:
    primary_client, primary_db_name = mongo_clients[0]
    db = load_db(primary_client, primary_db_name)
    if db is None:
        logger.error("Failed to load primary database. Exiting.")
        exit(1)
else:
    logger.error("No MongoDB connections established. Exiting.")
    exit(1)

# === USER COMMANDS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message for users."""
    user = update.effective_user
    await update.message.reply_text(
        f"Hello {user.first_name}, welcome to the Cricket Team Selection Bot! "
        f"Use /schedule to get started, /profile to view your bets, /yon for Yes/No questions, or /help for commands."
    )

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user commands."""
    help_text = (
        "User Commands\n\n"
        "Here are the commands you can use:\n"
        "/start - Start the bot and get a welcome message.\n"
        "/schedule - View available matches and select one to create a team or place a bet.\n"
        "/addamount <match_name> - Set a bet amount by choosing a room for a match (e.g., /addamount LSGvsCSK).\n"
        "/profile - View your teams, bets, and Yes/No answers.\n"
        "/yon - View and answer Yes/No questions.\n"
        "/yonrankings - View top 10 users based on correct Yes/No answers.\n\n"
        "For admins, use /admhelp to see admin commands."
    )
    await update.message.reply_text(help_text)

async def schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display available matches for users to select."""
    if not db["matches"]:
        await update.message.reply_text("No matches available.")
        return
    keyboard = [[InlineKeyboardButton(m, callback_data=f"user_match_{m}")] for m in db["matches"].keys()]
    await update.message.reply_text("Select a match:", reply_markup=InlineKeyboardMarkup(keyboard))

async def addamount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allow users to select a room for betting on a match."""
    user_id = str(update.effective_user.id)
    if len(context.args) < 1:
        keyboard = [[InlineKeyboardButton(m, callback_data=f"addamount::{m}")] for m in db["matches"].keys()]
        await update.message.reply_text("Select a match to set your bet amount:", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    match_name = context.args[0]
    if match_name not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    if locked_matches.get(match_name, False):
        await update.message.reply_text("❌ This match is locked. You can't place bets.")
        return
    keyboard = [
        [InlineKeyboardButton("Chotu (500)", callback_data=f"room::{match_name}::Chotu::500")],
        [InlineKeyboardButton("Rocket 🚀 (2500)", callback_data=f"room::{match_name}::Rocket::2500")]
    ]
    await update.message.reply_text(f"Choose a room for {match_name}:", reply_markup=InlineKeyboardMarkup(keyboard))

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display user's teams, bets, and Yes/No answers."""
    user_id = str(update.effective_user.id)
    msg = f"📋 *Your Profile* 📋\n\n"
    
    if user_id not in db["user_teams"] or not db["user_teams"][user_id]:
        msg += "No teams selected yet.\n"
    else:
        msg += "Your Teams:\n"
        for match, players in db["user_teams"][user_id].items():
            msg += f"{match}:\n"
            captain = db["captains"].get(user_id, {}).get(match, "Not selected")
            vice_captain = db["vice_captains"].get(user_id, {}).get(match, "Not selected")
            msg += f"Captain: {captain}\n"
            msg += f"Vice-Captain: {vice_captain}\n"
            msg += "Players:\n"
            for p in players:
                role = " (Captain)" if p == captain else " (Vice-Captain)" if p == vice_captain else ""
                msg += f"- {p}{role}\n"
            msg += "\n"
    
    if user_id not in db["amounts"] or not db["amounts"][user_id]:
        msg += "No bets placed yet.\n"
    else:
        msg += "Your Bets:\n"
        for match, amount in db["amounts"][user_id].items():
            msg += f"{match}: {amount} pokedollars\n"
    
    if user_id not in db["yon_user_answers"] or not db["yon_user_answers"][user_id]:
        msg += "No Yes/No answers submitted yet.\n"
    else:
        msg += "Your Yes/No Answers:\n"
        for qid, answer in db["yon_user_answers"][user_id].items():
            question = db["yon_questions"].get(qid, {}).get("question", "Unknown")
            msg += f"Q{qid}: {question} - {answer}\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def yonrankings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display top 10 users based on correct Yes/No answers, case-insensitive."""
    scores = {}
    for uid, answers in db["yon_user_answers"].items():
        total = 0
        for qid, answer in answers.items():
            correct = db["yon_correct_answers"].get(qid)
            if correct and answer.lower() == correct.lower():
                total += 1
        scores[uid] = total
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
    msg = "Yes/No Rankings (Top 10):\n"
    if not sorted_scores:
        msg += "No rankings available yet."
    for i, (uid, pts) in enumerate(sorted_scores, 1):
        msg += f"{i}. User {uid} - {pts} correct answers\n"
    await update.message.reply_text(msg)

async def yon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display Yes/No questions with navigation."""
    user_id = str(update.effective_user.id)
    if not db["yon_questions"]:
        await update.message.reply_text("No Yes/No questions available.")
        return
    question_ids = sorted(db["yon_questions"].keys())
    current_qid = question_ids[0]
    await display_yon_question(update, context, user_id, current_qid)

# === ADMIN COMMANDS ===
async def admhelp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display admin commands."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    help_text = (
        "Admin Commands\n\n"
        "Here are the commands available for admins:\n"
        "/admin - Open the admin panel to manage matches.\n"
        "/addmatch <match_name> - Add a new match (e.g., /addmatch LSGvsCSK).\n"
        "/removematch <match_name> - Remove a match and its players (e.g., /removematch LSGvsCSK).\n"
        "/addteam <match_name> <team_name> - Add a team to a match (e.g., /addteam LSGvsCSK LSG).\n"
        "/removeteam <match_name> <team_name> - Remove a team from a match (e.g., /removeteam LSGvsCSK LSG).\n"
        "/addplayer <match_name> <team_name> <players> - Add players to a team (e.g., /addplayer LSGvsCSK LSG Player1,Player2).\n"
        "/resetplayers <match_name> <team_name> - Reset players in a team (e.g., /resetplayers LSGvsCSK LSG).\n"
        "/points <player> <points> - Assign points to a player (e.g., /points Player1 100).\n"
        "/lockmatch <match_name> - Lock a match to prevent team edits or bets (e.g., /lockmatch LSGvsCSK).\n"
        "/unlockmatch <match_name> - Unlock a match to allow team edits and bets (e.g., /unlockmatch LSGvsCSK).\n"
        "/clear - Clear all data in MongoDB (use with caution!).\n"
        "/yonadd <question> <option1> <option2> - Add a Yes/No question (e.g., /yonadd \"Will it rain?\" \"Maybe yes\" \"Maybe low\").\n"
        "/yona <question_id> <option_number> - Set the correct answer for a Yes/No question (e.g., /yona 1 1).\n"
        "/yonclear - Clear all Yes/No questions and answers.\n"
        "/announcement <group_id> <message> - Send a message to a group (e.g., /announcement -100123456789 Match starts soon!).\n"
        "/team - View all users' teams with their user IDs for verification.\n"
        "/backup - Download the match data as a JSON file.\n"
        "/upload - Upload a JSON file to restore the database.\n"
        "/checkstorage - Manually check MongoDB storage and send notifications if low.\n\n"
        "Use /help to see user commands."
    )
    await update.message.reply_text(help_text)

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Open the admin panel to manage matches."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    keyboard = [[InlineKeyboardButton(m, callback_data=f"admin_match_{m}")] for m in db["matches"].keys()]
    await update.message.reply_text("Admin Panel - Matches:", reply_markup=InlineKeyboardMarkup(keyboard))

async def addmatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /addmatch <match_name>")
        return
    match = context.args[0]
    if match in db["matches"]:
        await update.message.reply_text("Match already exists.")
    else:
        db["matches"][match] = {"teams": {}, "players": []}
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await update.message.reply_text(f"Match {match} added.")

async def removematch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a match and its players, preserving user betting history for other matches."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Usage: /removematch <match_name>")
        return
    match = context.args[0]
    if match not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    # Remove the match and its players
    db["matches"].pop(match, None)
    # Remove match-specific user data, preserving other matches
    for user_id in db["user_teams"]:
        db["user_teams"][user_id].pop(match, None)
    for user_id in db["amounts"]:
        db["amounts"][user_id].pop(match, None)
    for user_id in db["pending_bets"]:
        db["pending_bets"][user_id].pop(match, None)
    for user_id in db["captains"]:
        db["captains"][user_id].pop(match, None)
    for user_id in db["vice_captains"]:
        db["vice_captains"][user_id].pop(match, None)
    locked_matches.pop(match, None)
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text(f"Match {match} and its players removed. User betting history for other matches preserved.")

async def addteam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a team to a match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addteam <match_name> <team_name>")
        return
    match, team = context.args[0], context.args[1]
    if match not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    db["matches"][match]["teams"][team] = []
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text(f"Team {team} added to {match}.")

async def removeteam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a team from a match."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /removeteam <match_name> <team_name>")
        return
    match, team = context.args[0], context.args[1]
    if match not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    if team not in db["matches"][match]["teams"]:
        await update.message.reply_text("Team not found.")
        return
    players = db["matches"][match]["teams"].pop(team, [])
    db["matches"][match]["players"] = [p for p in db["matches"][match]["players"] if p not in players]
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text(f"Team {team} removed from {match}.")

async def addplayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add players to a team."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addplayer <match_name> <team_name> <player1,player2,...>")
        return
    match, team = context.args[0], context.args[1]
    if match not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    if team not in db["matches"][match]["teams"]:
        await update.message.reply_text("Team not found.")
        return
    try:
        player_str = " ".join(context.args[2:])
        players = [p.strip().strip("(),") for p in player_str.split(",")]
        db["matches"][match]["teams"][team].extend(players)
        db["matches"][match]["players"].extend(players)
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await update.message.reply_text(f"Players added to {team} in {match}: {', '.join(players)}")
    except Exception as e:
        logger.error(f"Failed to add players: {e}")
        await update.message.reply_text("Failed to parse players.")

async def resetplayers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset players in a team."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /resetplayers <match_name> <team_name>")
        return
    match, team = context.args[0], context.args[1]
    if match not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    if team not in db["matches"][match]["teams"]:
        await update.message.reply_text("Team not found.")
        return
    players = db["matches"][match]["teams"][team]
    db["matches"][match]["players"] = [p for p in db["matches"][match]["players"] if p not in players]
    db["matches"][match]["teams"][team] = []
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text(f"Players reset for {team} in {match}.")

async def points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Assign points to a player."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /points <player> <points>")
        return
    player, pts = context.args[0], context.args[1]
    try:
        pts = int(pts)
        db["points"][player] = pts
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await update.message.reply_text(f"{player} got {pts} points.")
    except ValueError:
        await update.message.reply_text("Points must be a number.")

async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all data in MongoDB across all databases."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    # Reset in-memory database
    db.clear()
    db.update({
        "matches": {},
        "user_teams": {},
        "points": {},
        "amounts": {},
        "yon_questions": {},
        "yon_user_answers": {},
        "yon_correct_answers": {},
        "pending_bets": {},
        "captains": {},
        "vice_captains": {}
    })
    # Drop and recreate collection in all MongoDB databases
    for client, db_name in mongo_clients:
        try:
            db_mongo = client[db_name]
            db_mongo[COLLECTION_NAME].drop()
            db_mongo[COLLECTION_NAME].insert_one(db.copy())
        except Exception as e:
            logger.error(f"Failed to clear database {db_name}: {e}")
    locked_matches.clear()
    await update.message.reply_text("All data cleared across all MongoDB databases.")

async def lock_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lock a match to prevent edits or bets."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /lockmatch <match_name>")
        return
    match_name = context.args[0]
    if match_name not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    locked_matches[match_name] = True
    await update.message.reply_text(f"✅ Match '{match_name}' has been locked.")

async def unlock_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unlock a match to allow edits and bets."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /unlockmatch <match_name>")
        return
    match_name = context.args[0]
    if match_name not in db["matches"]:
        await update.message.reply_text("Match not found.")
        return
    if match_name not in locked_matches:
        await update.message.reply_text(f"Match '{match_name}' is not locked.")
        return
    locked_matches.pop(match_name, None)
    await update.message.reply_text(f"✅ Match '{match_name}' has been unlocked.")

async def yonadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a Yes/No question with custom options."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Usage: /yonadd \"<question>\" \"<option1>\" \"<option2>\"\n"
            "Example: /yonadd \"Will it rain?\" \"Maybe yes\" \"Maybe low\""
        )
        return
    
    args_str = " ".join(context.args)
    try:
        parts = []
        current = ""
        in_quotes = False
        for char in args_str:
            if char == '"' and not in_quotes:
                in_quotes = True
            elif char == '"' and in_quotes:
                in_quotes = False
                parts.append(current)
                current = ""
            elif in_quotes:
                current += char
            elif char == " " and not in_quotes and current:
                parts.append(current)
                current = ""
            elif not in_quotes and char != " ":
                current += char
        if current:
            parts.append(current)
        
        if len(parts) < 3:
            await update.message.reply_text(
                "Please provide a question and two options.\n"
                "Example: /yonadd \"Will it rain?\" \"Maybe yes\" \"Maybe low\""
            )
            return
        
        question = parts[0].strip()
        option1 = parts[-2].strip()
        option2 = parts[-1].strip()
        if not args_str.startswith('"'):
            question = " ".join(parts[:-2]).strip()
            option1 = parts[-2].strip()
            option2 = parts[-1].strip()
        
        if not question or not option1 or not option2:
            await update.message.reply_text("Invalid format: Question and options cannot be empty.")
            return
        
        question_id = str(len(db["yon_questions"]) + 1)
        db["yon_questions"][question_id] = {
            "question": question,
            "options": [option1, option2],
            "options_lower": [option1.lower(), option2.lower()]
        }
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await update.message.reply_text(f"Question {question_id} added: {question} ({option1}/{option2})")
    except Exception as e:
        logger.error(f"Failed to parse /yonadd command: {e}")
        await update.message.reply_text(
            "Failed to parse command. Use format: /yonadd \"question\" \"option1\" \"option2\"\n"
            "Example: /yonadd \"Will it rain?\" \"Maybe yes\" \"Maybe low\""
        )

async def yona(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the correct answer for a Yes/No question."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /yona <question_id> <option_number>\nExample: /yona 1 1")
        return
    qid, option_number = context.args
    if qid not in db["yon_questions"]:
        await update.message.reply_text("Question not found.")
        return
    try:
        option_number = int(option_number)
        if option_number not in [1, 2]:
            await update.message.reply_text("Option number must be 1 or 2.")
            return
    except ValueError:
        await update.message.reply_text("Option number must be a number (1 or 2).")
        return
    option_index = option_number - 1
    correct_option = db["yon_questions"][qid]["options"][option_index]
    db["yon_correct_answers"][qid] = correct_option
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text(f"Correct answer for Q{qid} set to: {correct_option}")

async def yonclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all Yes/No questions and answers."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    db["yon_questions"].clear()
    db["yon_user_answers"].clear()
    db["yon_correct_answers"].clear()
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    await update.message.reply_text("All Yes/No questions and answers cleared.")

async def announcement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message to a specific group."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /announcement <group_id> <message>")
        return
    group_id = context.args[0]
    message = " ".join(context.args[1:])
    try:
        group_id = int(group_id)
        await context.bot.send_message(
            chat_id=group_id,
            text=f"📢 *Announcement*: {message}",
            parse_mode="Markdown"
        )
        await update.message.reply_text(f"Announcement sent to group {group_id}.")
    except ValueError:
        await update.message.reply_text("Invalid group ID. It must be a number (e.g., -100123456789).")
    except TelegramError as e:
        logger.error(f"Failed to send announcement to group {group_id}: {e}")
        await update.message.reply_text(f"Failed to send announcement: {e.message}")

async def team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display all users' teams with user IDs for admin verification."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    
    if not db["user_teams"]:
        await update.message.reply_text("No users have selected teams yet.")
        return
    
    msg = "📋 *User Teams for Verification* 📋\n\n"
    for user_id, matches in db["user_teams"].items():
        msg += f"User ID: {user_id}\n"
        for match, players in matches.items():
            msg += f"  Match: {match}\n"
            if not players:
                msg += "    No players selected.\n"
            else:
                captain = db["captains"].get(user_id, {}).get(match, "Not selected")
                vice_captain = db["vice_captains"].get(user_id, {}).get(match, "Not selected")
                msg += f"    Captain: {captain}\n"
                msg += f"    Vice-Captain: {vice_captain}\n"
                msg += "    Players:\n"
                for p in players:
                    role = " (Captain)" if p == captain else " (Vice-Captain)" if p == vice_captain else ""
                    msg += f"      - {p}{role}\n"
                if user_id in db["amounts"] and match in db["amounts"][user_id]:
                    msg += f"    Bet: {db['amounts'][user_id][match]} pokedollars\n"
            msg += "\n"
        msg += "-" * 20 + "\n"
    
    await update.message.reply_text(msg, parse_mode="Markdown")

async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download the match data as a JSON file."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            json_data = json.dumps(db, indent=2)
            temp_file.write(json_data.encode('utf-8'))
            temp_file_path = temp_file.name
        
        with open(temp_file_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename="match_data_backup.json",
                caption="Here is the backup of the match data."
            )
        os.unlink(temp_file_path)
    except Exception as e:
        logger.error(f"Failed to send backup: {e}")
        await update.message.reply_text("❌ Failed to generate backup. Please try again later.")

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Prompt admin to upload a JSON file to restore the database."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    await update.message.reply_text(
        "Please upload a JSON file (e.g., match_data_backup.json) to restore the database."
    )
    context.user_data["awaiting_upload"] = True

async def handle_uploaded_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle uploaded JSON file to restore the database."""
    user_id = update.effective_user.id
    if not is_admin(user_id) or not context.user_data.get("awaiting_upload", False):
        await update.message.reply_text("❌ You are not authorized or not in upload mode.")
        return
    
    context.user_data["awaiting_upload"] = False
    
    document = update.message.document
    if not document or not document.file_name.endswith(".json"):
        await update.message.reply_text("❌ Please upload a valid JSON file.")
        return
    
    try:
        file = await document.get_file()
        file_bytes = await file.download_as_bytearray()
        new_db = json.loads(file_bytes.decode("utf-8"))
        
        required_keys = [
            "matches", "user_teams", "points", "amounts", "yon_questions",
            "yon_user_answers", "yon_correct_answers", "pending_bets",
            "captains", "vice_captains"
        ]
        if not all(key in new_db for key in required_keys):
            await update.message.reply_text("❌ Invalid JSON format: Missing required keys.")
            return
        
        global db
        db = new_db
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await update.message.reply_text("✅ Database restored successfully.")
    except json.JSONDecodeError:
        await update.message.reply_text("❌ Invalid JSON file: Could not parse the file.")
    except Exception as e:
        logger.error(f"Failed to restore database: {e}")
        await update.message.reply_text("❌ Failed to restore database. Please try again.")

# === YES/NO QUESTION DISPLAY ===
async def display_yon_question(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: str, qid: str):
    """Display a Yes/No question with navigation buttons."""
    query = update.callback_query if update.callback_query else None
    question_data = db["yon_questions"].get(qid, {})
    if not question_data:
        await (query.edit_message_text if query else update.message.reply_text)("Question not found.")
        return

    question = question_data["question"]
    options = question_data["options"]
    correct_answer = db["yon_correct_answers"].get(qid)
    user_answer = db["yon_user_answers"].get(user_id, {}).get(qid)

    msg = f"Q{qid}: {question}\n"
    msg += f"Options:\n1. {options[0]}\n2. {options[1]}\n\n"
    if user_answer:
        msg += f"Your answer: {user_answer}\n"
    if correct_answer:
        msg += f"Correct answer: {correct_answer}\n"

    keyboard = []
    if not user_answer and not correct_answer:
        keyboard.append([
            InlineKeyboardButton(f"1. {options[0]}", callback_data=f"yon_answer::{qid}::{options[0]}"),
            InlineKeyboardButton(f"2. {options[1]}", callback_data=f"yon_answer::{qid}::{options[1]}")
        ])

    question_ids = sorted(db["yon_questions"].keys())
    current_index = question_ids.index(qid)
    nav_buttons = []
    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton("Previous", callback_data=f"yon_nav::{question_ids[current_index - 1]}"))
    if current_index < len(question_ids) - 1:
        nav_buttons.append(InlineKeyboardButton("Next", callback_data=f"yon_nav::{question_ids[current_index + 1]}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    if query:
        await query.edit_message_text(msg, reply_markup=reply_markup)
    else:
        await update.message.reply_text(msg, reply_markup=reply_markup)

async def yon_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Yes/No question answers."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    _, qid, answer = query.data.split("::")
    
    if qid not in db["yon_questions"]:
        await query.edit_message_text("Question not found.")
        return
    if db["yon_correct_answers"].get(qid):
        await query.answer("This question has already been answered.", show_alert=True)
        return
    
    db["yon_user_answers"].setdefault(user_id, {})[qid] = answer
    for client, db_name in mongo_clients:
        save_db(client, db_name, db)
    
    question_ids = sorted(db["yon_questions"].keys())
    current_index = question_ids.index(qid)
    next_qid = question_ids[current_index + 1] if current_index + 1 < len(question_ids) else qid
    
    await display_yon_question(update, context, user_id, next_qid)

async def yon_nav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Navigate between Yes/No questions."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    _, qid = query.data.split("::")
    await display_yon_question(update, context, user_id, qid)

# === CALLBACK HANDLER ===
async def user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks."""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)

    if data.startswith("admin_match_"):
        match = data.replace("admin_match_", "")
        keyboard = [
            [InlineKeyboardButton("Add Team", callback_data=f"admin_addteam_{match}")],
            [InlineKeyboardButton("Add Players", callback_data=f"admin_addplayer_{match}")]
        ]
        await query.edit_message_text(f"Admin Panel for {match}:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("user_match_"):
        match = data.replace("user_match_", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        captain = db.get("captains", {}).get(user_id, {}).get(match, "Not selected")
        vice_captain = db.get("vice_captains", {}).get(user_id, {}).get(match, "Not selected")
        keyboard = [
            [InlineKeyboardButton("Create Team", callback_data=f"create_{match}")],
            [InlineKeyboardButton("Add Bet", callback_data=f"addamount::{match}")]
        ]
        if current_team:
            keyboard.append([InlineKeyboardButton("Choose Captain", callback_data=f"choosecaptain::{match}")])
        if len(current_team) >= 2:
            keyboard.append([InlineKeyboardButton("Choose Vice-Captain", callback_data=f"choosevc::{match}")])
        await query.edit_message_text(
            f"🏏 Match: {match} 🏏\n=====\n"
            f"👑 Captain: {captain}\n"
            f"⭐ Vice-Captain: {vice_captain}\n"
            f"🏃 Players: {', '.join(current_team) if current_team else 'None'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("addamount::"):
        match = data.replace("addamount::", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't place bets.", show_alert=True)
            return
        keyboard = [
            [InlineKeyboardButton("Chotu (500)", callback_data=f"room::{match}::Chotu::500")],
            [InlineKeyboardButton("Rocket 🚀 (2500)", callback_data=f"room::{match}::Rocket::2500")]
        ]
        await query.edit_message_text(f"Choose a room for {match}:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("room::"):
        _, match, room, amount = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't place bets.", show_alert=True)
            return
        if not db["user_teams"].get(user_id, {}).get(match):
            await query.edit_message_text(
                f"You haven't created a team for {match} yet. Please create a team first using 'Create Team'.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Create Team", callback_data=f"create_{match}")]
                ])
            )
            return
        amount = int(amount)
        db["pending_bets"].setdefault(user_id, {})[match] = {"room": room, "amount": amount}
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        user = query.from_user
        username = user.username or f"{user.first_name} {user.last_name or ''}".strip()
        try:
            await context.bot.send_message(
                chat_id=VERIFICATION_GROUP_ID,
                text=f"🏏 {username} (ID: {user_id}) has created a team and is requesting {amount} pokedollars to bet in the {room} lobby for {match}. 🏏\n=====\nPlease verify the bet.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Verify", callback_data=f"verify_bet::{user_id}::{match}::{amount}")]
                ])
            )
        except TelegramError as e:
            logger.error(f"Failed to send verification message to group: {e}")
            await query.edit_message_text("Failed to send verification request. Please try again.")
            return
        await query.edit_message_text("Verification message sent. Awaiting admin approval.")
        await context.bot.send_message(
            chat_id=user_id,
            text="Please tag @Trainwr_OFFicial in the group to proceed with verification."
        )

    elif data.startswith("verify_bet::"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Only admins can verify bets.", show_alert=True)
            return
        _, target_user_id, match, amount = data.split("::")
        amount = int(amount)
        if not db.get("pending_bets", {}).get(target_user_id, {}).get(match):
            await query.edit_message_text("Bet request not found or already verified.")
            return
        db["amounts"].setdefault(target_user_id, {})[match] = amount
        db["pending_bets"][target_user_id].pop(match, None)
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        await query.edit_message_text(f"Bet of {amount} pokedollars verified for User {target_user_id} in {match}.")
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"Your bet of {amount} pokedollars for {match} has been verified."
            )
        except TelegramError as e:
            logger.error(f"Failed to notify user {target_user_id}: {e}")

    elif data.startswith("create_"):
        match = data.replace("create_", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        db["user_teams"].setdefault(user_id, {}).setdefault(match, [])
        keyboard = []
        for team in db["matches"][match]["teams"]:
            keyboard.append([InlineKeyboardButton(team, callback_data=f"selectteam::{match}::{team}::select")])
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"user_match_{match}")])
        await query.edit_message_text("Choose team to select players:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("selectteam::"):
        _, match, team, mode = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        players = db["matches"][match]["teams"].get(team, [])
        if not players:
            await query.edit_message_text(f"No players available for team {team}.")
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        keyboard = []
        if mode == "select":
            for player in players:
                if player in current_team:
                    keyboard.append([InlineKeyboardButton(f"{player} ✅", callback_data=f"already_selected::{match}")])
                else:
                    keyboard.append([InlineKeyboardButton(player, callback_data=f"selectplayer::{match}::{team}::{player}")])
        else:
            for player in players:
                if player in current_team:
                    keyboard.append([InlineKeyboardButton(player, callback_data=f"removeplayer::{match}::{player}::{team}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"{player} 🚫", callback_data=f"not_in_team::{match}")])
        keyboard.append([
            InlineKeyboardButton(
                "Continue Making Team" if mode == "remove" else "Remove Player",
                callback_data=f"toggle_remove::{match}::{team}::{'select' if mode == 'remove' else 'remove'}"
            )
        ])
        keyboard.append([InlineKeyboardButton("Back to Team Selection", callback_data=f"create_{match}")])
        keyboard.append([InlineKeyboardButton("Back to Match Menu", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Select players from {team} ({len(current_team)}/11):",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("toggle_remove::"):
        _, match, team, new_mode = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        players = db["matches"][match]["teams"].get(team, [])
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        keyboard = []
        if new_mode == "select":
            for player in players:
                if player in current_team:
                    keyboard.append([InlineKeyboardButton(f"{player} ✅", callback_data=f"already_selected::{match}")])
                else:
                    keyboard.append([InlineKeyboardButton(player, callback_data=f"selectplayer::{match}::{team}::{player}")])
        else:
            for player in players:
                if player in current_team:
                    keyboard.append([InlineKeyboardButton(player, callback_data=f"removeplayer::{match}::{player}::{team}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"{player} 🚫", callback_data=f"not_in_team::{match}")])
        keyboard.append([
            InlineKeyboardButton(
                "Continue Making Team" if new_mode == "remove" else "Remove Player",
                callback_data=f"toggle_remove::{match}::{team}::{'select' if new_mode == 'remove' else 'remove'}"
            )
        ])
        keyboard.append([InlineKeyboardButton("Back to Team Selection", callback_data=f"create_{match}")])
        keyboard.append([InlineKeyboardButton("Back to Match Menu", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Select players from {team} ({len(current_team)}/11):",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("already_selected::"):
        await query.answer("This player is already in your team.", show_alert=True)

    elif data.startswith("not_in_team::"):
        await query.answer("This player is not in your team.", show_alert=True)

    elif data.startswith("selectplayer::"):
        _, match, team, player = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        user_teams = db["user_teams"].setdefault(user_id, {}).setdefault(match, [])
        if len(user_teams) < 11 and player not in user_teams:
            user_teams.append(player)
            for client, db_name in mongo_clients:
                save_db(client, db_name, db)
        players = db["matches"][match]["teams"].get(team, [])
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        keyboard = []
        for p in players:
            if p in current_team:
                keyboard.append([InlineKeyboardButton(f"{p} ✅", callback_data=f"already_selected::{match}")])
            else:
                keyboard.append([InlineKeyboardButton(p, callback_data=f"selectplayer::{match}::{team}::{p}")])
        keyboard.append([InlineKeyboardButton("Remove Player", callback_data=f"toggle_remove::{match}::{team}::remove")])
        keyboard.append([InlineKeyboardButton("Back to Team Selection", callback_data=f"create_{match}")])
        keyboard.append([InlineKeyboardButton("Back to Match Menu", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Select players from {team} ({len(current_team)}/11):",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("removeplayer::"):
        _, match, player, team = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        user_team = db["user_teams"].get(user_id, {}).get(match, [])
        if player in user_team:
            user_team.remove(player)
            if db.get("captains", {}).get(user_id, {}).get(match) == player:
                db["captains"].setdefault(user_id, {}).pop(match, None)
            if db.get("vice_captains", {}).get(user_id, {}).get(match) == player:
                db["vice_captains"].setdefault(user_id, {}).pop(match, None)
            for client, db_name in mongo_clients:
                save_db(client, db_name, db)
        players = db["matches"][match]["teams"].get(team, [])
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        keyboard = []
        for p in players:
            if p in current_team:
                keyboard.append([InlineKeyboardButton(p, callback_data=f"removeplayer::{match}::{p}::{team}")])
            else:
                keyboard.append([InlineKeyboardButton(f"{p} 🚫", callback_data=f"not_in_team::{match}")])
        keyboard.append([InlineKeyboardButton("Continue Making Team", callback_data=f"toggle_remove::{match}::{team}::select")])
        keyboard.append([InlineKeyboardButton("Back to Team Selection", callback_data=f"create_{match}")])
        keyboard.append([InlineKeyboardButton("Back to Match Menu", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Select players from {team} ({len(current_team)}/11):",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("choosecaptain::"):
        match = data.replace("choosecaptain::", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        if not current_team:
            await query.edit_message_text("No players in your team to choose as Captain.")
            return
        keyboard = [
            [InlineKeyboardButton(player, callback_data=f"selectcaptain::{match}::{player}")]
            for player in current_team
        ]
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Choose Captain for {match}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("selectcaptain::"):
        _, match, player = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        if player not in current_team:
            await query.edit_message_text("Player not in your team.")
            return
        db["captains"].setdefault(user_id, {})[match] = player
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        captain = db.get("captains", {}).get(user_id, {}).get(match, "Not selected")
        vice_captain = db.get("vice_captains", {}).get(user_id, {}).get(match, "Not selected")
        keyboard = [
            [InlineKeyboardButton("Create Team", callback_data=f"create_{match}")],
            [InlineKeyboardButton("Add Bet", callback_data=f"addamount::{match}")]
        ]
        if current_team:
            keyboard.append([InlineKeyboardButton("Choose Captain", callback_data=f"choosecaptain::{match}")])
        if len(current_team) >= 2:
            keyboard.append([InlineKeyboardButton("Choose Vice-Captain", callback_data=f"choosevc::{match}")])
        await query.edit_message_text(
            f"🏏 Match: {match} 🏏\n=====\n"
            f"👑 Captain: {captain}\n"
            f"⭐ Vice-Captain: {vice_captain}\n"
            f"🏃 Players: {', '.join(current_team) if current_team else 'None'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("choosevc::"):
        match = data.replace("choosevc::", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        captain = db.get("captains", {}).get(user_id, {}).get(match, None)
        if not current_team or len(current_team) < 2:
            await query.edit_message_text("You need at least two players in your team to choose a Vice-Captain.")
            return
        keyboard = [
            [InlineKeyboardButton(player, callback_data=f"selectvc::{match}::{player}")]
            for player in current_team if player != captain
        ]
        keyboard.append([InlineKeyboardButton("Back", callback_data=f"back::{match}")])
        await query.edit_message_text(
            f"Choose Vice-Captain for {match}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("selectvc::"):
        _, match, player = data.split("::")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        if locked_matches.get(match, False):
            await query.answer("❌ This match is locked. You can't make changes.", show_alert=True)
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        captain = db.get("captains", {}).get(user_id, {}).get(match, None)
        if player not in current_team:
            await query.edit_message_text("Player not in your team.")
            return
        if player == captain:
            await query.edit_message_text("Player is already the Captain.")
            return
        db["vice_captains"].setdefault(user_id, {})[match] = player
        for client, db_name in mongo_clients:
            save_db(client, db_name, db)
        captain = db.get("captains", {}).get(user_id, {}).get(match, "Not selected")
        vice_captain = db.get("vice_captains", {}).get(user_id, {}).get(match, "Not selected")
        keyboard = [
            [InlineKeyboardButton("Create Team", callback_data=f"create_{match}")],
            [InlineKeyboardButton("Add Bet", callback_data=f"addamount::{match}")]
        ]
        if current_team:
            keyboard.append([InlineKeyboardButton("Choose Captain", callback_data=f"choosecaptain::{match}")])
        if len(current_team) >= 2:
            keyboard.append([InlineKeyboardButton("Choose Vice-Captain", callback_data=f"choosevc::{match}")])
        await query.edit_message_text(
            f"🏏 Match: {match} 🏏\n=====\n"
            f"👑 Captain: {captain}\n"
            f"⭐ Vice-Captain: {vice_captain}\n"
            f"🏃 Players: {', '.join(current_team) if current_team else 'None'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("back::"):
        match = data.replace("back::", "")
        if match not in db["matches"]:
            await query.edit_message_text("Match not found.")
            return
        current_team = db["user_teams"].get(user_id, {}).get(match, [])
        captain = db.get("captains", {}).get(user_id, {}).get(match, "Not selected")
        vice_captain = db.get("vice_captains", {}).get(user_id, {}).get(match, "Not selected")
        keyboard = [
            [InlineKeyboardButton("Create Team", callback_data=f"create_{match}")],
            [InlineKeyboardButton("Add Bet", callback_data=f"addamount::{match}")]
        ]
        if current_team:
            keyboard.append([InlineKeyboardButton("Choose Captain", callback_data=f"choosecaptain::{match}")])
        if len(current_team) >= 2:
            keyboard.append([InlineKeyboardButton("Choose Vice-Captain", callback_data=f"choosevc::{match}")])
        await query.edit_message_text(
            f"🏏 Match: {match} 🏏\n=====\n"
            f"👑 Captain: {captain}\n"
            f"⭐ Vice-Captain: {vice_captain}\n"
            f"🏃 Players: {', '.join(current_team) if current_team else 'None'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("yon_answer::"):
        await yon_answer(update, context)
    
    elif data.startswith("yon_nav::"):
        await yon_nav(update, context)

# === MAIN ===
def main():
    """Start the bot."""
    application = Application.builder().token(BOT_TOKEN).build()

    # User commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help))
    application.add_handler(CommandHandler("schedule", schedule))
    application.add_handler(CommandHandler("addamount", addamount))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("yon", yon))
    application.add_handler(CommandHandler("yonrankings", yonrankings))

    # Admin commands
    application.add_handler(CommandHandler("admhelp", admhelp))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("addmatch", addmatch))
    application.add_handler(CommandHandler("removematch", removematch))
    application.add_handler(CommandHandler("addteam", addteam))
    application.add_handler(CommandHandler("removeteam", removeteam))
    application.add_handler(CommandHandler("addplayer", addplayer))
    application.add_handler(CommandHandler("resetplayers", resetplayers))
    application.add_handler(CommandHandler("points", points))
    application.add_handler(CommandHandler("clear", clear))
    application.add_handler(CommandHandler("lockmatch", lock_match))
    application.add_handler(CommandHandler("unlockmatch", unlock_match))
    application.add_handler(CommandHandler("yonadd", yonadd))
    application.add_handler(CommandHandler("yona", yona))
    application.add_handler(CommandHandler("yonclear", yonclear))
    application.add_handler(CommandHandler("announcement", announcement))
    application.add_handler(CommandHandler("team", team))
    application.add_handler(CommandHandler("backup", backup))
    application.add_handler(CommandHandler("upload", upload))
    application.add_handler(CommandHandler("checkstorage", check_storage))

    # Callback and file upload handlers
    application.add_handler(CallbackQueryHandler(user_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_uploaded_file))

    # Start storage monitoring
    application.run_polling()

if __name__ == "__main__":
    main()
