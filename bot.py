import asyncio
import logging
import sqlite3
import requests
import re
import csv
import os
import time
import aiohttp
import json
import gc
import psutil
import glob
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ================= STORAGE-AWARE SETTINGS =================
MAX_STORAGE_MB = 300  # Maximum storage for payment data
RESERVED_STORAGE_MB = 50  # Reserved for group member analysis
BATCH_SIZE = 80
MAX_CONCURRENT_TASKS = 2
CSV_DOWNLOAD_INTERVAL = 10800  # 3 hours
GROUP_CHECK_INTERVAL = 10800  # 3 hours (same as download interval)
MAX_CSV_FILES_TO_KEEP = 2
MEMORY_LIMIT_MB = 350
# ==========================================================

# Bot configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8216881905:AAFo0Lnufs8crn2IZ-p8gSaaxV3QK-i0KLs')
ADMIN_IDS = [int(id.strip()) for id in os.environ.get('ADMIN_IDS', '8085393860').split(',')]
GROUP_CHAT_ID = int(os.environ.get('GROUP_CHAT_ID', '-1002965409390'))

# Encryption setup
try:
    from cryptography.fernet import Fernet
    ENCRYPTION_KEY = Fernet.generate_key()
    cipher = Fernet(ENCRYPTION_KEY)
except ImportError:
    logger.warning("Cryptography not available, using basic encoding")
    cipher = None

# Database setup
def init_database():
    conn = sqlite3.connect('payments.db', check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute('PRAGMA journal_mode=WAL')
    cursor.execute('PRAGMA synchronous = NORMAL')
    cursor.execute('PRAGMA cache_size = -2000')
    
    # Existing tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        joined_date TIMESTAMP,
        chat_id INTEGER,
        in_group BOOLEAN DEFAULT FALSE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payments (
        payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        currency TEXT,
        status TEXT,
        transaction_id TEXT UNIQUE,
        selar_order_id TEXT UNIQUE,
        payment_date TIMESTAMP,
        subscription_type TEXT,
        expires_date TIMESTAMP,
        verification_sent BOOLEAN DEFAULT FALSE,
        processed_order BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS group_members (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        last_checked TIMESTAMP,
        status TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS download_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        download_time TIMESTAMP,
        filename TEXT,
        file_size_mb REAL,
        payments_processed INTEGER,
        status TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS storage_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        check_time TIMESTAMP,
        total_used_mb REAL,
        csv_files_mb REAL,
        database_mb REAL,
        available_mb REAL
    )
    ''')
    
    # Create index for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(selar_order_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_expires ON payments(expires_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_in_group ON users(in_group)')
    
    conn.commit()
    conn.close()

init_database()

class PaymentBot:
    def __init__(self):
        self.memory_limit_mb = MEMORY_LIMIT_MB
        self.start_time = datetime.now()
        self.total_payments_processed = 0
        self.last_memory_check = time.time()
        self.last_group_check = time.time()
        
        self.application = Application.builder().token(BOT_TOKEN).build()
        self.setup_handlers()
        self.background_tasks = set()
        self.processing = False
        self.session = None
        self.awaiting_credentials = {}
        
        self.selar_email, self.selar_password = self.load_credentials()
        self.credentials_configured = bool(self.selar_email and self.selar_password)
        
        self.start_background_tasks()
    
    def load_credentials(self):
        """Load credentials from file if exists"""
        try:
            if os.path.exists('credentials.json'):
                with open('credentials.json', 'r') as f:
                    data = json.load(f)
                    return data.get('email'), data.get('password')
        except:
            pass
        return None, None
    
    def save_credentials(self, email, password):
        """Save credentials to file"""
        try:
            with open('credentials.json', 'w') as f:
                json.dump({'email': email, 'password': password}, f)
            return True
        except:
            return False
    
    def setup_handlers(self):
        """Setup bot command handlers"""
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("verify", self.verify_payment))
        self.application.add_handler(CommandHandler("mystatus", self.my_status))
        self.application.add_handler(CommandHandler("stats", self.stats))
        self.application.add_handler(CommandHandler("configure", self.configure_credentials))
        self.application.add_handler(CommandHandler("download", self.download_csv))
        self.application.add_handler(CommandHandler("process", self.process_payments))
        self.application.add_handler(CommandHandler("storage", self.storage_status))
        self.application.add_handler(CommandHandler("group_stats", self.group_stats))
        self.application.add_handler(CommandHandler("add_missing", self.add_missing_users))
        self.application.add_handler(CommandHandler("remove_expired", self.remove_expired_users))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send a message when the command /start is issued."""
        user = update.effective_user
        await update.message.reply_text(
            f"Hello {user.first_name}! I'm a payment verification bot.\n\n"
            "Use /mystatus to check your subscription status\n"
            "Use /verify <transaction_id> to verify a payment"
        )
    
    async def my_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Check user's subscription status"""
        user_id = update.effective_user.id
        
        try:
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            
            # Get user's active subscription
            cursor.execute('''
            SELECT subscription_type, expires_date, payment_date, status 
            FROM payments 
            WHERE user_id = ? AND status = 'completed'
            ORDER BY payment_date DESC 
            LIMIT 1
            ''', (user_id,))
            
            subscription = cursor.fetchone()
            conn.close()
            
            if subscription:
                sub_type, expires_date, payment_date, status = subscription
                
                # Calculate time remaining
                now = datetime.now()
                if isinstance(expires_date, str):
                    expires_date = datetime.strptime(expires_date, '%Y-%m-%d %H:%M:%S')
                
                time_remaining = expires_date - now
                
                if time_remaining.total_seconds() <= 0:
                    status_text = "❌ Your subscription has EXPIRED"
                else:
                    # Calculate years, months, days, hours, minutes, seconds
                    days = time_remaining.days
                    years = days // 365
                    months = (days % 365) // 30
                    remaining_days = (days % 365) % 30
                    hours = time_remaining.seconds // 3600
                    minutes = (time_remaining.seconds % 3600) // 60
                    seconds = time_remaining.seconds % 60
                    
                    status_text = f"""
✅ Your Subscription Status:

📦 Type: {sub_type.capitalize()}
📅 Started: {payment_date}
⏰ Expires: {expires_date}
🟢 Status: {status}

⏳ Time Remaining:
- Years: {years}
- Months: {months}
- Days: {remaining_days}
- Hours: {hours}
- Minutes: {minutes}
- Seconds: {seconds}

💡 Your access to the group will be automatically managed based on your subscription status.
"""
            else:
                status_text = "❌ No active subscription found. Please make a payment to access the group."
            
            await update.message.reply_text(status_text)
            
        except Exception as e:
            logger.error(f"Error checking user status: {e}")
            await update.message.reply_text("❌ Error checking your status. Please try again later.")
    
    async def verify_payment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Verify a payment using transaction ID"""
        if len(context.args) == 0:
            await update.message.reply_text("Please provide a transaction ID. Usage: /verify <transaction_id>")
            return
        
        transaction_id = context.args[0]
        conn = sqlite3.connect('payments.db')
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT p.*, u.username, u.first_name, u.last_name 
        FROM payments p 
        JOIN users u ON p.user_id = u.user_id 
        WHERE p.transaction_id = ? OR p.selar_order_id = ?
        ''', (transaction_id, transaction_id))
        
        payment = cursor.fetchone()
        conn.close()
        
        if payment:
            status_text = f"""
✅ Payment Verified

👤 User: {payment[8]} {payment[9]} (@{payment[7]})
💰 Amount: {payment[2]} {payment[3]}
📅 Date: {payment[6]}
🆔 Transaction ID: {payment[5]}
📦 Order ID: {payment[4]}
📝 Status: {payment[3]}
🔐 Subscription: {payment[8]}
⏰ Expires: {payment[9]}
            """
            await update.message.reply_text(status_text)
        else:
            await update.message.reply_text("❌ Payment not found. Please check the transaction ID.")
    
    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show bot statistics"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        conn = sqlite3.connect('payments.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM payments')
        total_payments = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM payments WHERE status = "completed"')
        completed_payments = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE in_group = TRUE')
        users_in_group = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM group_members')
        group_members = cursor.fetchone()[0]
        
        # Get expired subscriptions count
        cursor.execute('SELECT COUNT(*) FROM payments WHERE expires_date < datetime("now") AND status = "completed"')
        expired_subscriptions = cursor.fetchone()[0]
        
        conn.close()
        
        stats_text = f"""
📊 Bot Statistics

💳 Payments:
- Total: {total_payments}
- Completed: {completed_payments}
- Expired: {expired_subscriptions}

👥 Users:
- In Group: {users_in_group}
- Total Members: {group_members}

🕒 Uptime: {str(datetime.now() - self.start_time).split('.')[0]}
        """
        
        await update.message.reply_text(stats_text)
    
    async def configure_credentials(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Configure Selar credentials"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        self.awaiting_credentials[user_id] = True
        await update.message.reply_text("Please send your Selar credentials in the format: email:password")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming messages"""
        user_id = update.effective_user.id
        
        if user_id in self.awaiting_credentials and self.awaiting_credentials[user_id]:
            try:
                text = update.message.text
                if ':' in text:
                    email, password = text.split(':', 1)
                    self.selar_email = email.strip()
                    self.selar_password = password.strip()
                    
                    if self.save_credentials(self.selar_email, self.selar_password):
                        self.credentials_configured = True
                        await update.message.reply_text("✅ Credentials saved successfully!")
                    else:
                        await update.message.reply_text("❌ Failed to save credentials.")
                    
                    self.awaiting_credentials[user_id] = False
                else:
                    await update.message.reply_text("❌ Invalid format. Please use: email:password")
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {e}")
                self.awaiting_credentials[user_id] = False
    
    async def download_csv(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Download CSV from Selar"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        if not self.credentials_configured:
            await update.message.reply_text("❌ Credentials not configured. Use /configure first.")
            return
        
        await update.message.reply_text("⏳ Downloading CSV file...")
        success, filename = await self.download_with_storage_management()
        
        if success:
            file_size_mb = os.path.getsize(filename) / 1024 / 1024 if os.path.exists(filename) else 0
            
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO download_history (download_time, filename, file_size_mb, payments_processed, status)
            VALUES (?, ?, ?, ?, ?)
            ''', (datetime.now(), filename, file_size_mb, 0, 'downloaded'))
            conn.commit()
            conn.close()
            
            await update.message.reply_text(f"✅ CSV downloaded: {filename} ({file_size_mb:.2f} MB)")
        else:
            await update.message.reply_text(f"❌ Failed to download CSV: {filename}")
    
    async def process_payments(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process payments from downloaded CSV files"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        if self.processing:
            await update.message.reply_text("⏳ Already processing payments. Please wait.")
            return
        
        self.processing = True
        await update.message.reply_text("⏳ Processing payments...")
        
        try:
            csv_files = glob.glob("selar_export_*.csv")
            if not csv_files:
                await update.message.reply_text("❌ No CSV files found. Download first with /download")
                self.processing = False
                return
            
            processed_count = 0
            for csv_file in csv_files:
                count = await self.process_csv_file(csv_file)
                processed_count += count
            
            # Update download history
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            for csv_file in csv_files:
                cursor.execute('''
                UPDATE download_history 
                SET payments_processed = payments_processed + ?, status = 'processed'
                WHERE filename = ?
                ''', (processed_count, csv_file))
            conn.commit()
            conn.close()
            
            # After processing, check for users to add/remove
            await self.check_group_members()
            added_count = await self.add_missing_users_to_group()
            removed_count = await self.remove_expired_users_from_group()
            
            await update.message.reply_text(
                f"✅ Processed {processed_count} payments from {len(csv_files)} files.\n"
                f"✅ Added {added_count} missing users to group.\n"
                f"✅ Removed {removed_count} expired users from group."
            )
            
        except Exception as e:
            logger.error(f"Error processing payments: {e}")
            await update.message.reply_text(f"❌ Error processing payments: {e}")
        
        self.processing = False
    
    async def process_csv_file(self, filename):
        """Process a single CSV file"""
        processed_count = 0
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                conn = sqlite3.connect('payments.db')
                cursor = conn.cursor()
                
                for row in reader:
                    try:
                        # Extract order ID first (this should never be deleted)
                        selar_order_id = row.get('order_id', '') or row.get('selar_order_id', '')
                        if not selar_order_id:
                            continue
                            
                        # Check if this order has already been processed
                        cursor.execute('SELECT COUNT(*) FROM payments WHERE selar_order_id = ?', (selar_order_id,))
                        if cursor.fetchone()[0] > 0:
                            continue  # Skip already processed orders
                        
                        # Extract user info from the row
                        user_id = int(row.get('user_id', 0)) or int(row.get('telegram_id', 0))
                        if not user_id:
                            continue
                        
                        username = row.get('username', '')
                        first_name = row.get('first_name', '')
                        last_name = row.get('last_name', '')
                        amount = float(row.get('amount', 0))
                        currency = row.get('currency', 'USD')
                        status = row.get('status', 'completed')
                        transaction_id = row.get('transaction_id', '')
                        payment_date_str = row.get('payment_date', '')
                        subscription_type = row.get('subscription_type', 'one_time')
                        
                        # Parse payment date
                        try:
                            payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d %H:%M:%S')
                        except:
                            payment_date = datetime.now()
                        
                        # Calculate expiration date based on subscription type
                        if subscription_type.lower() == 'monthly':
                            expires_date = payment_date + timedelta(days=29, hours=24)
                        elif subscription_type.lower() == 'yearly':
                            expires_date = payment_date + timedelta(days=364, hours=24)
                        else:
                            expires_date = payment_date + timedelta(days=365*10)  # 10 years for one-time
                        
                        # Insert or update user
                        cursor.execute('''
                        INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, joined_date)
                        VALUES (?, ?, ?, ?, ?)
                        ''', (user_id, username, first_name, last_name, datetime.now()))
                        
                        # Insert payment with order ID tracking
                        cursor.execute('''
                        INSERT INTO payments 
                        (user_id, amount, currency, status, transaction_id, selar_order_id, 
                         payment_date, subscription_type, expires_date, processed_order)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (user_id, amount, currency, status, transaction_id, selar_order_id, 
                              payment_date, subscription_type, expires_date, True))
                        
                        processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing row: {e}")
                        continue
                
                conn.commit()
                conn.close()
                
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {e}")
        
        return processed_count
    
    def calculate_storage_usage(self):
        """Calculate current storage usage and available space"""
        try:
            # Calculate CSV files size
            csv_files = glob.glob("selar_export_*.csv")
            csv_size_mb = sum(os.path.getsize(f) for f in csv_files) / 1024 / 1024
            
            # Calculate database size
            db_size_mb = os.path.getsize('payments.db') / 1024 / 1024 if os.path.exists('payments.db') else 0
            
            total_used_mb = csv_size_mb + db_size_mb
            available_mb = MAX_STORAGE_MB - total_used_mb
            
            # Record storage usage
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO storage_usage (check_time, total_used_mb, csv_files_mb, database_mb, available_mb)
            VALUES (?, ?, ?, ?, ?)
            ''', (datetime.now(), total_used_mb, csv_size_mb, db_size_mb, available_mb))
            conn.commit()
            conn.close()
            
            return {
                'total_used_mb': total_used_mb,
                'csv_files_mb': csv_size_mb,
                'database_mb': db_size_mb,
                'available_mb': available_mb
            }
            
        except Exception as e:
            logger.error(f"Error calculating storage usage: {e}")
            return None
    
    def should_download_more_data(self):
        """Check if we have space for more downloads"""
        storage_info = self.calculate_storage_usage()
        if not storage_info:
            return True  # Continue on error
        
        # Reserve 50MB for group analysis
        return storage_info['available_mb'] > RESERVED_STORAGE_MB
    
    def cleanup_oldest_csv(self):
        """Delete oldest CSV file to free up space"""
        try:
            csv_files = glob.glob("selar_export_*.csv")
            if not csv_files:
                return False
                
            # Sort by modification time (oldest first)
            csv_files.sort(key=os.path.getmtime)
            oldest_file = csv_files[0]
            
            # Delete the file
            file_size_mb = os.path.getsize(oldest_file) / 1024 / 1024
            os.remove(oldest_file)
            logger.info(f"Freed {file_size_mb:.2f} MB by deleting {oldest_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting oldest CSV: {e}")
            return False
    
    async def download_selar_csv(self):
        """Download CSV from Selar (implementation depends on Selar API)"""
        # This is a placeholder - you'll need to implement the actual Selar API integration
        try:
            # Simulate download
            filename = f"selar_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            with open(filename, 'w') as f:
                f.write("order_id,user_id,username,first_name,last_name,amount,currency,status,payment_date,subscription_type,transaction_id\n")
                # Add sample data
                f.write("ORD123,123456789,testuser,Test,User,10.00,USD,completed,2023-01-01 12:00:00,monthly,TXN123\n")
            
            return True, filename
        except Exception as e:
            logger.error(f"Error downloading CSV: {e}")
            return False, str(e)
    
    async def download_with_storage_management(self):
        """Download CSV with storage awareness"""
        if not self.should_download_more_data():
            logger.warning("Storage limit reached, cleaning up before download")
            if not self.cleanup_oldest_csv():
                logger.error("Could not free up space for new download")
                return False, "Storage limit reached"
        
        # Proceed with download
        return await self.download_selar_csv()
    
    async def check_group_members(self):
        """Check all group members and update database"""
        try:
            logger.info("Starting group member check")
            
            # Get all group members
            group_members = []
            try:
                async for member in self.application.bot.get_chat_members(GROUP_CHAT_ID):
                    group_members.append({
                        'user_id': member.user.id,
                        'username': member.user.username,
                        'first_name': member.user.first_name,
                        'last_name': member.user.last_name,
                        'status': member.status
                    })
            except Exception as e:
                logger.error(f"Error fetching group members: {e}")
                return
            
            # Update group members in database
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            
            # Clear existing group members
            cursor.execute('DELETE FROM group_members')
            
            # Insert current members
            for member in group_members:
                cursor.execute('''
                INSERT INTO group_members (user_id, username, first_name, last_name, last_checked, status)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    member['user_id'], member['username'], member['first_name'],
                    member['last_name'], datetime.now(), member['status']
                ))
            
            # Update user records with group status
            cursor.execute('UPDATE users SET in_group = FALSE')
            
            user_ids_in_group = [str(m['user_id']) for m in group_members]
            if user_ids_in_group:
                placeholders = ','.join('?' * len(user_ids_in_group))
                cursor.execute(f'UPDATE users SET in_group = TRUE WHERE user_id IN ({placeholders})', user_ids_in_group)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated {len(group_members)} group members in database")
            
        except Exception as e:
            logger.error(f"Error in group member check: {e}")
    
    async def find_missing_group_members(self):
        """Find paid users who are not in the group"""
        try:
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            
            # Find users with completed payments but not in group
            cursor.execute('''
            SELECT u.user_id, u.username, u.first_name, u.last_name, p.selar_order_id
            FROM users u
            JOIN payments p ON u.user_id = p.user_id
            WHERE p.status = 'completed' AND u.in_group = FALSE
            AND p.expires_date > datetime('now')
            ''')
            
            missing_users = cursor.fetchall()
            conn.close()
            
            return missing_users
            
        except Exception as e:
            logger.error(f"Error finding missing group members: {e}")
            return []
    
    async def add_missing_users_to_group(self):
        """Add users who have paid but are not in the group"""
        missing_users = await self.find_missing_group_members()
        
        if not missing_users:
            logger.info("No missing users found")
            return 0
        
        added_count = 0
        for user_id, username, first_name, last_name, order_id in missing_users:
            try:
                # Check if user is already in group (double-check)
                try:
                    chat_member = await self.application.bot.get_chat_member(GROUP_CHAT_ID, user_id)
                    if chat_member.status in ['member', 'administrator', 'creator']:
                        # User is actually in group, update database
                        conn = sqlite3.connect('payments.db')
                        cursor = conn.cursor()
                        cursor.execute('UPDATE users SET in_group = TRUE WHERE user_id = ?', (user_id,))
                        conn.commit()
                        conn.close()
                        continue
                except:
                    pass  # User is not in group
                
                # Add user to group
                await self.application.bot.add_chat_member(GROUP_CHAT_ID, user_id)
                await asyncio.sleep(1)  # Rate limiting
                
                # Update database
                conn = sqlite3.connect('payments.db')
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET in_group = TRUE WHERE user_id = ?', (user_id,))
                conn.commit()
                conn.close()
                
                added_count += 1
                logger.info(f"Added missing user to group: {user_id} ({first_name} {last_name})")
                
                # Send welcome message
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=f"👋 Welcome to the group, {first_name}! Your payment has been verified."
                    )
                except:
                    pass  # Could not send message
                    
            except Exception as e:
                logger.error(f"Error adding user {user_id} to group: {e}")
        
        return added_count
    
    async def find_expired_users_in_group(self):
        """Find users with expired subscriptions who are still in the group"""
        try:
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT u.user_id, u.username, u.first_name, u.last_name, p.subscription_type, p.expires_date, p.selar_order_id
            FROM users u
            JOIN payments p ON u.user_id = p.user_id
            WHERE u.in_group = TRUE 
            AND p.expires_date < datetime('now')
            AND p.status = 'completed'
            ''')
            
            expired_users = cursor.fetchall()
            conn.close()
            return expired_users
            
        except Exception as e:
            logger.error(f"Error finding expired users: {e}")
            return []
    
    async def remove_expired_users_from_group(self):
        """Remove users with expired subscriptions from the group"""
        expired_users = await self.find_expired_users_in_group()
        
        if not expired_users:
            logger.info("No expired users found in the group.")
            return 0
        
        removed_count = 0
        
        for user_id, username, first_name, last_name, sub_type, expires_date, order_id in expired_users:
            try:
                # Remove user from group
                await self.application.bot.ban_chat_member(GROUP_CHAT_ID, user_id)
                await asyncio.sleep(1)  # Rate limiting
                
                # Update database
                conn = sqlite3.connect('payments.db')
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET in_group = FALSE WHERE user_id = ?', (user_id,))
                conn.commit()
                conn.close()
                
                removed_count += 1
                logger.info(f"Removed expired user from group: {user_id} (Order: {order_id})")
                
                # Send notification to user
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=f"❌ Your {sub_type} subscription has expired. You've been removed from the group. Please renew to regain access."
                    )
                except:
                    pass  # Could not send message
                    
            except Exception as e:
                logger.error(f"Error removing user {user_id}: {e}")
        
        return removed_count
    
    async def remove_expired_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove users with expired subscriptions from the group (admin command)"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        expired_users = await self.find_expired_users_in_group()
        
        if not expired_users:
            await update.message.reply_text("✅ No expired users found in the group.")
            return 0
        
        removed_count = 0
        message = "🔍 Removing expired users:\n\n"
        
        for user_id, username, first_name, last_name, sub_type, expires_date, order_id in expired_users:
            try:
                # Remove user from group
                await self.application.bot.ban_chat_member(GROUP_CHAT_ID, user_id)
                await asyncio.sleep(1)  # Rate limiting
                
                # Update database
                conn = sqlite3.connect('payments.db')
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET in_group = FALSE WHERE user_id = ?', (user_id,))
                conn.commit()
                conn.close()
                
                removed_count += 1
                message += f"❌ Removed {first_name} {last_name} (@{username}) - {sub_type} expired on {expires_date} (Order: {order_id})\n"
                logger.info(f"Removed expired user from group: {user_id} (Order: {order_id})")
                
                # Send notification to user
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=f"❌ Your {sub_type} subscription has expired. You've been removed from the group. Please renew to regain access."
                    )
                except:
                    pass  # Could not send message
                    
            except Exception as e:
                logger.error(f"Error removing user {user_id}: {e}")
                message += f"⚠️ Failed to remove {first_name} {last_name}: {e}\n"
        
        message += f"\n✅ Removed {removed_count} expired users."
        await update.message.reply_text(message)
        return removed_count
    
    async def automated_group_management(self):
        """Automated group management every 3 hours"""
        while True:
            try:
                # Wait for next check
                await asyncio.sleep(GROUP_CHECK_INTERVAL)
                
                # Check storage before proceeding
                storage_info = self.calculate_storage_usage()
                if storage_info and storage_info['available_mb'] < 10:
                    logger.warning("Low storage available, skipping group check")
                    continue
                
                logger.info("Starting automated group management")
                
                # 1. Check current group members
                await self.check_group_members()
                
                # 2. Find and add missing users
                added_count = await self.add_missing_users_to_group()
                
                # 3. Find and remove expired users
                removed_count = await self.remove_expired_users_from_group()
                
                if added_count > 0 or removed_count > 0:
                    logger.info(f"Added {added_count} missing users, removed {removed_count} expired users")
                    # Notify admin
                    try:
                        await self.application.bot.send_message(
                            chat_id=ADMIN_IDS[0],
                            text=f"🔄 Group management:\n✅ Added {added_count} missing users\n❌ Removed {removed_count} expired users"
                        )
                    except:
                        pass
                
                # 4. Clean up old data if needed
                storage_info = self.calculate_storage_usage()
                if storage_info and storage_info['available_mb'] < RESERVED_STORAGE_MB:
                    self.cleanup_oldest_csv()
                
            except Exception as e:
                logger.error(f"Error in automated group management: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
    
    async def automated_csv_downloader(self):
        """Automated CSV download with storage management"""
        while True:
            try:
                if not self.credentials_configured:
                    await asyncio.sleep(CSV_DOWNLOAD_INTERVAL)
                    continue
                
                # Wait for next download
                await asyncio.sleep(CSV_DOWNLOAD_INTERVAL)
                
                # Check storage and cleanup if needed
                if not self.should_download_more_data():
                    logger.warning("Storage limit reached, cleaning up before download")
                    if not self.cleanup_oldest_csv():
                        logger.error("Could not free up space for download")
                        continue
                
                # Download with storage management
                success, filename = await self.download_with_storage_management()
                
                if success:
                    # Record download with file size
                    file_size_mb = os.path.getsize(filename) / 1024 / 1024 if os.path.exists(filename) else 0
                    
                    conn = sqlite3.connect('payments.db')
                    cursor = conn.cursor()
                    cursor.execute('''
                    INSERT INTO download_history (download_time, filename, file_size_mb, payments_processed, status)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (datetime.now(), filename, file_size_mb, 0, 'downloaded'))
                    conn.commit()
                    conn.close()
                    
                    logger.info(f"Downloaded CSV: {filename} ({file_size_mb:.2f} MB)")
                    
            except Exception as e:
                logger.error(f"Error in automated CSV download: {e}")
                await asyncio.sleep(300)
    
    async def check_expiring_subscriptions(self):
        """Check for subscriptions that will expire soon"""
        while True:
            try:
                await asyncio.sleep(86400)  # Check once per day
                
                conn = sqlite3.connect('payments.db')
                cursor = conn.cursor()
                
                # Find subscriptions expiring in the next 3 days
                cursor.execute('''
                SELECT u.user_id, u.username, u.first_name, p.subscription_type, p.expires_date
                FROM users u
                JOIN payments p ON u.user_id = p.user_id
                WHERE p.expires_date BETWEEN datetime('now') AND datetime('now', '+3 days')
                AND p.status = 'completed'
                ''')
                
                expiring_users = cursor.fetchall()
                conn.close()
                
                for user_id, username, first_name, sub_type, expires_date in expiring_users:
                    try:
                        await self.application.bot.send_message(
                            chat_id=user_id,
                            text=f"⚠️ Your {sub_type} subscription will expire on {expires_date}. Please renew to maintain access to the group."
                        )
                    except:
                        pass  # Could not send message
                
            except Exception as e:
                logger.error(f"Error checking expiring subscriptions: {e}")
    
    async def check_expired_subscriptions(self):
        """Background task to check for expired subscriptions"""
        while True:
            try:
                await asyncio.sleep(43200)  # Check twice per day
                
                # Find and remove expired users
                expired_users = await self.find_expired_users_in_group()
                removed_count = 0
                
                for user_id, username, first_name, last_name, sub_type, expires_date, order_id in expired_users:
                    try:
                        # Remove user from group
                        await self.application.bot.ban_chat_member(GROUP_CHAT_ID, user_id)
                        await asyncio.sleep(1)  # Rate limiting
                        
                        # Update database
                        conn = sqlite3.connect('payments.db')
                        cursor = conn.cursor()
                        cursor.execute('UPDATE users SET in_group = FALSE WHERE user_id = ?', (user_id,))
                        conn.commit()
                        conn.close()
                        
                        removed_count += 1
                        logger.info(f"Automatically removed expired user from group: {user_id} (Order: {order_id})")
                        
                        # Send notification to user
                        try:
                            await self.application.bot.send_message(
                                chat_id=user_id,
                                text=f"❌ Your {sub_type} subscription has expired. You've been removed from the group. Please renew to regain access."
                            )
                        except:
                            pass  # Could not send message
                            
                    except Exception as e:
                        logger.error(f"Error automatically removing user {user_id}: {e}")
                
                if removed_count > 0:
                    logger.info(f"Automatically removed {removed_count} expired users")
                
            except Exception as e:
                logger.error(f"Error in expired subscription check: {e}")
    
    def start_background_tasks(self):
        """Start all background tasks"""
        loop = asyncio.get_event_loop()
        
        # CSV download task
        task1 = loop.create_task(self.automated_csv_downloader())
        self.background_tasks.add(task1)
        task1.add_done_callback(self.background_tasks.discard)
        
        # Group management task
        task2 = loop.create_task(self.automated_group_management())
        self.background_tasks.add(task2)
        task2.add_done_callback(self.background_tasks.discard)
        
        # Subscription tasks
        task3 = loop.create_task(self.check_expiring_subscriptions())
        self.background_tasks.add(task3)
        task3.add_done_callback(self.background_tasks.discard)
        
        task4 = loop.create_task(self.check_expired_subscriptions())
        self.background_tasks.add(task4)
        task4.add_done_callback(self.background_tasks.discard)
    
    async def storage_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Check current storage usage"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        storage_info = self.calculate_storage_usage()
        if not storage_info:
            await update.message.reply_text("❌ Could not calculate storage usage.")
            return
        
        status_text = f"""
💾 **Storage Status**

📊 **Usage:**
- Total Used: {storage_info['total_used_mb']:.2f} MB / {MAX_STORAGE_MB} MB
- CSV Files: {storage_info['csv_files_mb']:.2f} MB
- Database: {storage_info['database_mb']:.2f} MB
- Available: {storage_info['available_mb']:.2f} MB

🎯 **Limits:**
- Max Storage: {MAX_STORAGE_MB} MB
- Reserved for Analysis: {RESERVED_STORAGE_MB} MB
- Can Download More: {'✅ Yes' if storage_info['available_mb'] > RESERVED_STORAGE_MB else '❌ No'}

🔄 **Next Actions:**
- Next CSV Download: {CSV_DOWNLOAD_INTERVAL/3600} hours
- Next Group Check: {GROUP_CHECK_INTERVAL/3600} hours
        """
        
        await update.message.reply_text(status_text)
    
    async def group_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show group statistics"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        try:
            conn = sqlite3.connect('payments.db')
            cursor = conn.cursor()
            
            # Get group member count
            cursor.execute('SELECT COUNT(*) FROM group_members')
            group_count = cursor.fetchone()[0]
            
            # Get paid users not in group
            cursor.execute('''
            SELECT COUNT(*) FROM users u
            JOIN payments p ON u.user_id = p.user_id
            WHERE p.status = 'completed' AND u.in_group = FALSE
            AND p.expires_date > datetime('now')
            ''')
            missing_count = cursor.fetchone()[0]
            
            # Get expired users still in group
            cursor.execute('''
            SELECT COUNT(*) FROM users u
            JOIN payments p ON u.user_id = p.user_id
            WHERE u.in_group = TRUE 
            AND p.expires_date < datetime('now')
            AND p.status = 'completed'
            ''')
            expired_count = cursor.fetchone()[0]
            
            # Get last check time
            cursor.execute('SELECT MAX(last_checked) FROM group_members')
            last_check = cursor.fetchone()[0]
            
            conn.close()
            
            stats_text = f"""
👥 **Group Statistics**

📊 **Membership:**
- Users in Group: {group_count}
- Paid Users Missing from Group: {missing_count}
- Expired Users Still in Group: {expired_count}
- Last Group Check: {last_check or 'Never'}

🔍 **Analysis:**
- Storage Reserved for Analysis: {RESERVED_STORAGE_MB} MB
- Check Interval: {GROUP_CHECK_INTERVAL/3600} hours

⚡ **Actions:**
- Use /add_missing to add missing users now
- Use /remove_expired to remove expired users now
- Next auto-check: {GROUP_CHECK_INTERVAL/3600} hours
            """
            
            await update.message.reply_text(stats_text)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error getting group stats: {e}")
    
    async def add_missing_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Manually add missing users to group"""
        user_id = update.effective_user.id
        
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ Access denied. Admin only.")
            return
        
        await update.message.reply_text("🔍 Searching for missing users...")
        
        added_count = await self.add_missing_users_to_group()
        
        if added_count > 0:
            await update.message.reply_text(f"✅ Added {added_count} missing users to group")
        else:
            await update.message.reply_text("✅ No missing users found")

    def run(self):
        """Run the bot"""
        self.application.run_polling()

if __name__ == "__main__":
    bot = PaymentBot()
    bot.run()
