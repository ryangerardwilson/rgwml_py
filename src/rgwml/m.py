import os
import json
import inspect
import sqlite3
import requests
import time
import re
import secrets
from datetime import datetime, timedelta
from flask import request, jsonify, make_response
from google.oauth2 import id_token, service_account
from google.auth.transport import requests as google_requests
from googleapiclient.discovery import build
import base64
import random
import string
from email.mime.text import MIMEText


class m:

    def google_auth(self, invoking_function_name, request, google_client_id, sqlite_db_path, post_authentication_redirect_url,
                    telegram_bot_preset_name):

        def verify_token(token, google_client_id):
            try:
                idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), google_client_id)
                return idinfo
            except ValueError:
                return None

        def create_tables(conn):
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        google_auth_user_id TEXT UNIQUE,
                        email TEXT UNIQUE,
                        name TEXT,
                        picture TEXT,
                        password TEXT,
                        active_token TEXT,
                        creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_login_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        latitude REAL,
                        longitude REAL,
                        FOREIGN KEY (user_id) REFERENCES user (id)
                    )
                ''')
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

        request_body_json = request.json
        token = request_body_json.get('token')
        user_latitude = request_body_json.get('userLatitude')
        user_longitude = request_body_json.get('userLongitude')
        
        try:
            if token:
                idinfo = verify_token(token, google_client_id)
                if idinfo:
                    self.send_telegram_message(invoking_function_name, "STEP1", telegram_bot_preset_name)
                    google_auth_user_id = idinfo['sub']
                    email = idinfo.get('email', '')
                    name = idinfo.get('name', '')
                    picture = idinfo.get('picture', '')
                    id_in_user_table = None
                    email_in_user_table = None

                    conn = sqlite3.connect(sqlite_db_path)
                    create_tables(conn)

                    self.send_telegram_message(invoking_function_name, "STEP2", telegram_bot_preset_name)

                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            "INSERT INTO user (google_auth_user_id, email, name, picture) VALUES (?, ?, ?, ?) "
                            "ON CONFLICT(google_auth_user_id) DO UPDATE SET email=excluded.email, name=excluded.name, picture=excluded.picture",
                            (google_auth_user_id, email, name, picture))
                        self.send_telegram_message(invoking_function_name, "STEP3", telegram_bot_preset_name)

                        cursor.execute("SELECT id, email, name FROM user WHERE google_auth_user_id = ?", (google_auth_user_id,))
                        result = cursor.fetchone()
                        id_in_user_table = result[0]
                        email_in_user_table = result[1]
                        name_in_user_table = result[2]

                        self.send_telegram_message(invoking_function_name, "STEP4", telegram_bot_preset_name)

                        cursor.execute("INSERT INTO user_login_logs (user_id, latitude, longitude) VALUES (?, ?, ?)",
                                       (id_in_user_table, user_latitude, user_longitude))

                        self.send_telegram_message(invoking_function_name, "STEP5", telegram_bot_preset_name)
                        conn.commit()

                    except Exception as e:
                        conn.rollback()
                        raise e
                    finally:
                        conn.close()

                    response_data = {
                        'success': True,
                        'user_id': id_in_user_table,
                        'user_email': email_in_user_table,
                        'user_name': name_in_user_table,
                        'url': post_authentication_redirect_url
                    }
                    response = make_response(jsonify(response_data), 200)
                    return response
                else:
                    return jsonify({'success': False, 'message': 'Invalid token'}), 400
            else:
                return jsonify({'success': False, 'message': 'Token not provided'}), 400
        except Exception as e:
            error_message = f"[ERROR::{invoking_function_name}] Error: {e}"
            self.insert_log(invoking_function_name, error_message, sqlite_db_path, telegram_bot_preset_name)
            self.send_telegram_message(invoking_function_name, error_message, telegram_bot_preset_name)
            return jsonify({"error": error_message}), 500



    def validate_email(self, invoking_function_name, request, sqlite_db_path, gmail_bot_preset_name, telegram_bot_preset_name):
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        def is_valid_email(email):
            regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
            return re.match(regex, email)

        def create_tables_if_not_exist(cursor):
            cursor.execute('''CREATE TABLE IF NOT EXISTS user (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                google_auth_user_id TEXT UNIQUE,
                                email TEXT UNIQUE,
                                name TEXT,
                                picture TEXT,
                                password TEXT,
                                active_token TEXT,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_login_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    latitude REAL,
                    longitude REAL,
                    FOREIGN KEY (user_id) REFERENCES user (id)
                )
            ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_pending_validation (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                email TEXT UNIQUE,
                                temp_password TEXT,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )''')

        def generate_temp_password(length=12):
            characters = string.ascii_letters + string.digits + string.punctuation
            return ''.join(random.choice(characters) for i in range(length))

        def send_email(sender_email_id, service_account_credentials_path, recipient_email, subject, message_text):
            def authenticate_service_account(service_account_credentials_path, sender_email_id):
                """Authenticate the service account and return a Gmail API service instance."""
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_credentials_path,
                    scopes=['https://www.googleapis.com/auth/gmail.send'],
                    subject=sender_email_id
                )
                service = build('gmail', 'v1', credentials=credentials)
                return service

            service = authenticate_service_account(service_account_credentials_path, sender_email_id)
            message = MIMEText(message_text)
            message['to'] = recipient_email
            message['from'] = sender_email_id
            message['subject'] = subject
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
            email_body = {'raw': raw}
            message = service.users().messages().send(userId="me", body=email_body).execute()
        # Load configuration
        config = load_config()
        service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
        sender_email_id = gmail_bot_preset_name

        # Extract email from the request
        request_body_json = request.json
        email = request_body_json.get('email')

        # Validate email
        if not email:
            return jsonify(success=False, message="Email is required"), 400
        if not is_valid_email(email):
            return jsonify(success=False, message="Invalid email address"), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()

            # Create necessary tables if they do not exist
            create_tables_if_not_exist(cursor)

            # Check if the email exists in the user table and fetch user data
            cursor.execute("SELECT google_auth_user_id FROM user WHERE email = ?", (email,))
            user = cursor.fetchone()

            # If the email belongs to a Gmail address or the user has `google_auth_user_id` associated, ask to login via Gmail
            if (user and user[0]):  # user[0] corresponds to google_auth_user_id
                return jsonify(success=False, message="Please use 'Sign in with Gmail'. Your account is attached to your Gmail Id."), 400
            elif email.endswith('@gmail.com'):  # user[0] corresponds to google_auth_user_id
                return jsonify(success=False, message="Please use 'Sign in with Gmail' instead, when signing in with a Gmail Id"), 400
            elif user:
                return jsonify(success=True, message="User confirmed. Seek password"), 200

            # Generate a temporary password
            temp_password = generate_temp_password()
            # Look for existing email
            cursor.execute(
                """SELECT COUNT(*) FROM user_pending_validation WHERE email = ?""",
                (email,)
            )
            exists = cursor.fetchone()[0]


            # If email exists, seek the password
            if exists:
                cursor.execute(
                    "UPDATE user_pending_validation SET temp_password = ?, creation_time = CURRENT_TIMESTAMP WHERE email = ?",
                    (temp_password, email)
                )
            # If email does not exist, insert a new row
            else:
                cursor.execute(
                    """INSERT INTO user_pending_validation (email, temp_password)
                       VALUES (?, ?)""",
                    (email, temp_password)
                )

            conn.commit()
            # Send the temporary password via email
            email_subject = "Your Temporary Password"
            email_message = f"Your temporary password is: {temp_password}. Use this to verify your email. Then, you will be redirected to create a new password."

            # self.send_telegram_message(invoking_function_name, f"PRE EMAIL DEBUG: \n\n{sender_email_id} \n\n {service_account_credentials_path} \n\n {email} \n\n {email_subject} \n\n {email_message}", telegram_bot_preset_name)
            send_email(sender_email_id, service_account_credentials_path, email, email_subject, email_message)
            # self.send_telegram_message(invoking_function_name, debug_message, telegram_bot_preset_name)

            return jsonify(success=True, message="Temporary password sent. Check your email."), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()

    # Validates temp password
    def validate_password(self, invoking_function_name, request, sqlite_db_path, gmail_bot_preset_name, telegram_bot_preset_name):
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        # Load configuration
        config = load_config()
        service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
        sender_email_id = gmail_bot_preset_name

        # Extract email and temp_password from the request
        request_body_json = request.json
        email = request_body_json.get('email')
        temp_password = request_body_json.get('temp_password')

        # Validate input
        if not email or not temp_password:
            return jsonify(success=False, message="Email and temporary password are required"), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()
            
            # Check if the email and temp_password match with the records in the user_pending_validation table
            cursor.execute(
                """SELECT temp_password, creation_time
                   FROM user_pending_validation
                   WHERE email = ?""",
                (email,)
            )
            record = cursor.fetchone()
            
            if not record:
                return jsonify(success=False, message="Email not found. Please request a new temporary password."), 404

            stored_temp_password, creation_time = record
            
            if stored_temp_password != temp_password:
                return jsonify(success=False, message="Invalid temporary password."), 400

            # Remove Validation item from user_pending_validation once confirmed
            """
            cursor.execute(
                "DELETE FROM user_pending_validation WHERE email = ?",
                (email,)
            )
            """

            conn.commit()

            return jsonify(success=True, message="Temporary password validated. Please proceed to set a new password."), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()

    def validate_user_password(self, invoking_function_name, request, sqlite_db_path, post_authentication_redirect_url, gmail_bot_preset_name, telegram_bot_preset_name):
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        def generate_auth_token(length=32):
            characters = string.ascii_letters + string.digits
            return ''.join(random.choice(characters) for i in range(length))


        # Load configuration
        config = load_config()
        service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
        sender_email_id = gmail_bot_preset_name

        # Extract email, password, latitude and longitude from the request
        request_body_json = request.json
        email = request_body_json.get('email')
        password = request_body_json.get('password')
        user_latitude = request_body_json.get('userLatitude')
        user_longitude = request_body_json.get('userLongitude')

        # Validate input
        if not email or not password or not user_latitude or not user_longitude:
            return jsonify(success=False, message="Email, password, latitude, and longitude are required"), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()

            # Check if the email and password match with the records in the user table
            cursor.execute(
                """SELECT id, name, password
                   FROM user
                   WHERE email = ?""",
                (email,)
            )
            record = cursor.fetchone()

            if not record:
                return jsonify(success=False, message="Email not found."), 404

            user_id, user_name, stored_password = record

            if stored_password != password:
                return jsonify(success=False, message="Invalid password."), 400

            # Insert login log with user location
            cursor.execute("INSERT INTO user_login_logs (user_id, latitude, longitude) VALUES (?, ?, ?)",
                           (user_id, user_latitude, user_longitude))


            # Generate a new authToken
            auth_token = generate_auth_token()

            cursor.execute(
                """UPDATE user
                   SET active_token = ?
                   WHERE id = ?;
                """,
                (auth_token, user_id)
            )

            conn.commit()

            return jsonify(
                success=True,
                message="Password validated successfully.",
                user_id=user_id,
                user_email=email,
                user_name=user_name,
                authToken=auth_token,
                url=post_authentication_redirect_url
            ), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()







    def set_first_password(self, invoking_function_name, request, sqlite_db_path, post_authentication_redirect_url, gmail_bot_preset_name, telegram_bot_preset_name):

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        def send_email(sender_email_id, service_account_credentials_path, recipient_email, subject, message_text):
            def authenticate_service_account(service_account_credentials_path, sender_email_id):
                """Authenticate the service account and return a Gmail API service instance."""
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_credentials_path,
                    scopes=['https://www.googleapis.com/auth/gmail.send'],
                    subject=sender_email_id
                )
                service = build('gmail', 'v1', credentials=credentials)
                return service

            service = authenticate_service_account(service_account_credentials_path, sender_email_id)
            message = MIMEText(message_text)
            message['to'] = recipient_email
            message['from'] = sender_email_id
            message['subject'] = subject
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
            email_body = {'raw': raw}
            message = service.users().messages().send(userId="me", body=email_body).execute()

        def generate_auth_token(length=32):
            characters = string.ascii_letters + string.digits
            return ''.join(random.choice(characters) for i in range(length))

        def is_valid_password(password):
            # Password validation logic e.g., minimum length, contains special characters, etc.
            if len(password) < 8:
                return False
            return True  # You can add more complex validation logic here


        # Extract email, temp_password, new password, and user location from the request
        request_body_json = request.json
        email = request_body_json.get('email')
        temp_password = request_body_json.get('temp_password')
        new_password = request_body_json.get('new_password')
        user_latitude = request_body_json.get('userLatitude')
        user_longitude = request_body_json.get('userLongitude')

        # Validate input
        if not email or not temp_password or not new_password:
            return jsonify(success=False, message="Email, temporary password, and new password are required"), 400

        if not is_valid_password(new_password):
            return jsonify(success=False, message="Invalid password. Ensure it meets the complexity requirements."), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()

            # Load configuration
            config = load_config()
            service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
            sender_email_id = gmail_bot_preset_name

            # Check if the email and temp_password match with the records in the user_pending_validation table
            cursor.execute(
                """SELECT temp_password
                   FROM user_pending_validation
                   WHERE email = ?""",
                (email,)
            )
            record = cursor.fetchone()

            if not record:
                return jsonify(success=False, message="No temporary password found for this email. Please request a new one."), 404

            stored_temp_password = record[0]

            if stored_temp_password != temp_password:
                return jsonify(success=False, message="Invalid temporary password."), 400

            # Generate a new authToken
            auth_token = generate_auth_token()

            # Create or update user with the email, new password, and auth token
            cursor.execute(
                """INSERT INTO user (email, password, active_token)
                   VALUES (?, ?, ?)
                   ON CONFLICT(email) DO UPDATE SET password=excluded.password, active_token=excluded.active_token;
                """,
                (email, new_password, auth_token)
            )

            # Fetch the user_id and name of the newly created or updated user
            cursor.execute(
                """SELECT id, name
                   FROM user
                   WHERE email = ?;
                """,
                (email,)
            )
            user_record = cursor.fetchone()
            user_id = user_record[0]
            username = user_record[1]

            # Insert login log with user location
            cursor.execute("INSERT INTO user_login_logs (user_id, latitude, longitude) VALUES (?, ?, ?)",
                           (user_id, user_latitude, user_longitude))

            # Clean up pending validation once the password has been set
            cursor.execute(
                """DELETE FROM user_pending_validation
                   WHERE email = ?""",
                (email,)
            )

            conn.commit()

            # Send the new password via email
            email_subject = "Your New Password"
            email_message = f"Your new password has been set successfully. Your new password is: {new_password} \n\nPlease keep it safe."
            send_email(sender_email_id, service_account_credentials_path, email, email_subject, email_message)

            return jsonify(
                success=True,
                message="Password has been set and sent to your email successfully.",
                user_id=user_id,
                user_email=email,
                user_name=username,
                authToken=auth_token,
                url=post_authentication_redirect_url
            ), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()

    def insert_log(self, invoking_function_name, message, sqlite_db_path, telegram_bot_preset_name):

        def create_logs_table(sqlite_db_path):
            # Create the logs table if it does not exist
            try:
                conn = sqlite3.connect(sqlite_db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()

        try:
            # Ensure the logs table is created
            create_logs_table(sqlite_db_path)
            conn = sqlite3.connect(sqlite_db_path)
            c = conn.cursor()
            c.execute("INSERT INTO logs (message, created_at) VALUES (?, ?)", (message, datetime.now()))
            conn.commit()
            conn.close()
        except Exception as e:
            conn.rollback()
            error_message = f"[ERROR::{invoking_function_name}] Error: {e}"
            self.send_telegram_message(invoking_function_name, error_message, telegram_bot_preset_name)
            raise e
            # return json.dumps({"error": error_message})
        finally:
            conn.close()

    def send_telegram_message(self, invoking_function_name, message, preset_name):

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_telegram_preset(config, preset_name):
            presets = config.get("telegram_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_telegram_bot_details(config, preset_name):
            preset = get_telegram_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Telegram bot preset '{preset_name}' not found in the configuration file")

            bot_token = preset.get("bot_token")
            chat_id = preset.get("chat_id")

            if not bot_token or not chat_id:
                raise RuntimeError(
                    f"Telegram bot token or chat ID for '{preset_name}' not found in the configuration file"
                )

            return bot_token, chat_id

        config = load_config()
        bot_token, chat_id = get_telegram_bot_details(config, preset_name)
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}

        response = requests.post(url, json=payload)
        response.raise_for_status()

    def send_reset_password_link(self, invoking_function_name, request, sqlite_db_path, reset_password_page_url, gmail_bot_preset_name, telegram_bot_preset_name):
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        def is_valid_email(email):
            regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
            return re.match(regex, email)

        def create_tables_if_not_exist(cursor):
            cursor.execute('''CREATE TABLE IF NOT EXISTS user (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                google_auth_user_id TEXT UNIQUE,
                                email TEXT UNIQUE,
                                name TEXT,
                                picture TEXT,
                                password TEXT,
                                active_token TEXT,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_login_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    latitude REAL,
                    longitude REAL,
                    FOREIGN KEY (user_id) REFERENCES user (id)
                )
            ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_pending_validation (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                email TEXT UNIQUE,
                                temp_password TEXT,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS password_reset_tokens (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                email TEXT UNIQUE,
                                token TEXT,
                                expiration_time TIMESTAMP
                              )''')

        def generate_reset_token(length=32):
            return secrets.token_urlsafe(length)

        def send_email(sender_email_id, service_account_credentials_path, recipient_email, subject, message_text):
            def authenticate_service_account(service_account_credentials_path, sender_email_id):
                """Authenticate the service account and return a Gmail API service instance."""
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_credentials_path,
                    scopes=['https://www.googleapis.com/auth/gmail.send'],
                    subject=sender_email_id
                )
                service = build('gmail', 'v1', credentials=credentials)
                return service

            service = authenticate_service_account(service_account_credentials_path, sender_email_id)
            message = MIMEText(message_text)
            message['to'] = recipient_email
            message['from'] = sender_email_id
            message['subject'] = subject
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
            email_body = {'raw': raw}
            message = service.users().messages().send(userId="me", body=email_body).execute()

        # Load configuration
        config = load_config()
        service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
        sender_email_id = gmail_bot_preset_name

        # Extract email from the request
        request_body_json = request.json
        email = request_body_json.get('email')

        # Validate email
        if not email:
            return jsonify(success=False, message="Email is required"), 400
        if not is_valid_email(email):
            return jsonify(success=False, message="Invalid email address"), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()

            # Create necessary tables if they do not exist
            create_tables_if_not_exist(cursor)

            # Check if the email exists in the user table
            cursor.execute("SELECT * FROM user WHERE email = ?", (email,))
            user = cursor.fetchone()

            if not user:
                return jsonify(success=False, message="No user found with this email"), 404

            # Generate a unique reset token
            reset_token = generate_reset_token()
            expiration_time = datetime.now() + timedelta(hours=1)  # Token will be valid for 1 hour

            # Insert or update the reset token in the database
            cursor.execute(
                """INSERT INTO password_reset_tokens (email, token, expiration_time)
                   VALUES (?, ?, ?)
                   ON CONFLICT(email) DO UPDATE SET token=excluded.token, expiration_time=excluded.expiration_time""",
                (email, reset_token, expiration_time)
            )
            conn.commit()

            # Send the reset token via email
            reset_link = f"{reset_password_page_url}?token={reset_token}"
            email_subject = "Password Reset Request"
            email_message = f"Click on the following link to reset your password: {reset_link}\n\nThis link is valid for 1 hour."

            send_email(sender_email_id, service_account_credentials_path, email, email_subject, email_message)

            return jsonify(success=True, message="Password reset link sent. Check your email."), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()

    def reset_password_and_login(self, invoking_function_name, request, sqlite_db_path, post_authentication_redirect_url, gmail_bot_preset_name, telegram_bot_preset_name):
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config():
            config_path = locate_config_file()
            with open(config_path, "r") as file:
                return json.load(file)

        def get_gmail_bot_preset(config, preset_name):
            presets = config.get("gmail_bot_presets", [])
            for preset in presets:
                if preset.get("name") == preset_name:
                    return preset
            return None

        def get_gmail_bot_details(config, preset_name):
            preset = get_gmail_bot_preset(config, preset_name)
            if not preset:
                raise RuntimeError(f"Gmail bot preset '{preset_name}' not found in the configuration file")
            service_account_credentials_path = preset.get("service_account_credentials_path")
            if not service_account_credentials_path:
                raise RuntimeError(f"Gmail service_account_credentials_path for '{preset_name}' not found in the configuration file.")
            return service_account_credentials_path

        def send_email(sender_email_id, service_account_credentials_path, recipient_email, subject, message_text):
            def authenticate_service_account(service_account_credentials_path, sender_email_id):
                """Authenticate the service account and return a Gmail API service instance."""
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_credentials_path,
                    scopes=['https://www.googleapis.com/auth/gmail.send'],
                    subject=sender_email_id
                )
                service = build('gmail', 'v1', credentials=credentials)
                return service

            service = authenticate_service_account(service_account_credentials_path, sender_email_id)
            message = MIMEText(message_text)
            message['to'] = recipient_email
            message['from'] = sender_email_id
            message['subject'] = subject
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
            email_body = {'raw': raw}
            message = service.users().messages().send(userId="me", body=email_body).execute()

        def generate_auth_token(length=32):
            characters = string.ascii_letters + string.digits
            return ''.join(random.choice(characters) for i in range(length))

        def is_valid_password(password):
            # Password validation logic e.g., minimum length, contains special characters, etc.
            if len(password) < 8:
                return False
            return True  # You can add more complex validation logic here

        # Extract token, new password, and user location from the request
        request_body_json = request.json
        token = request_body_json.get('token')
        new_password = request_body_json.get('new_password')
        user_latitude = request_body_json.get('userLatitude')
        user_longitude = request_body_json.get('userLongitude')

        # Validate input
        if not token or not new_password:
            return jsonify(success=False, message="Token and new password are required"), 400

        if not is_valid_password(new_password):
            return jsonify(success=False, message="Invalid password. Ensure it meets the complexity requirements."), 400

        try:
            conn = sqlite3.connect(sqlite_db_path)
            cursor = conn.cursor()

            # Load configuration
            config = load_config()
            service_account_credentials_path = get_gmail_bot_details(config, gmail_bot_preset_name)
            sender_email_id = gmail_bot_preset_name

            # Check if the token exists and has not expired
            cursor.execute(
                """SELECT email, expiration_time
                   FROM password_reset_tokens
                   WHERE token = ?""",
                (token,)
            )
            record = cursor.fetchone()

            if not record:
                return jsonify(success=False, message="Invalid or expired token"), 400

            email, expiration_time = record

            if datetime.strptime(expiration_time, '%Y-%m-%d %H:%M:%S.%f') < datetime.now():
                return jsonify(success=False, message="Invalid or expired token"), 400

            # Generate a new authToken
            auth_token = generate_auth_token()

            # Create or update user with the email, new password, and auth token
            cursor.execute(
                """INSERT INTO user (email, password, active_token)
                   VALUES (?, ?, ?)
                   ON CONFLICT(email) DO UPDATE SET password=excluded.password, active_token=excluded.active_token;
                """,
                (email, new_password, auth_token)
            )

            # Fetch the user_id and name of the newly created or updated user
            cursor.execute(
                """SELECT id, name
                   FROM user
                   WHERE email = ?;
                """,
                (email,)
            )
            user_record = cursor.fetchone()
            user_id = user_record[0]
            username = user_record[1]

            # Insert login log with user location
            cursor.execute("INSERT INTO user_login_logs (user_id, latitude, longitude) VALUES (?, ?, ?)",
                           (user_id, user_latitude, user_longitude))

            # Clean up reset token once the password has been set
            cursor.execute(
                """DELETE FROM password_reset_tokens
                   WHERE token = ?""",
                (token,)
            )

            conn.commit()

            # Send the new password via email
            email_subject = "Your New Password"
            email_message = f"Your new password has been set successfully to: {new_password} \n\nPlease keep it safe."
            send_email(sender_email_id, service_account_credentials_path, email, email_subject, email_message)

            return jsonify(
                success=True,
                message="Password has been set and updated successfully.",
                user_id=user_id,
                user_email=email,
                user_name=username,
                authToken=auth_token,
                url=post_authentication_redirect_url
            ), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"Error: {e}", telegram_bot_preset_name)
            return jsonify(error=str(e)), 500

        finally:
            conn.close()

    def send_secure_data(self, invoking_function_name, request, secure_data, sqlite_db_path, telegram_bot_preset_name, google_client_id):

        def verify_google_token(token, google_client_id):
            try:
                idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), google_client_id)
                return idinfo  # Return user profile information
            except ValueError:
                return None

        def verify_email_token(user_id, token):
            try:
                # self.send_telegram_message(invoking_function_name, f"Debug: user_id: {user_id} \n\n token: {token}", telegram_bot_preset_name)
                conn = sqlite3.connect(sqlite_db_path)
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT active_token
                       FROM user
                       WHERE id = ?""",
                    (user_id,)
                )
                record = cursor.fetchone()
                # self.send_telegram_message(invoking_function_name, f"Debug: record[0]: {record[0]}", telegram_bot_preset_name)
                if record and record[0] == token:
                    return True
                return False
            except Exception as e:
                self.insert_log(invoking_function_name, f"Database error: {e}", sqlite_db_path, telegram_bot_preset_name)
                self.send_telegram_message(invoking_function_name, f"Database error: {e}", telegram_bot_preset_name)
                return False
            finally:
                conn.close()

        try:
            auth_header = request.headers.get('Authorization')
            auth_type = request.headers.get('AuthTokenType')
            user_id = request.headers.get('UserId')
            if auth_header and auth_header.startswith('Bearer '):
                secure_token = auth_header.split(' ')[1]  # Extract the token part
            else:
                secure_token = None

            if auth_type == 'google_auth':
                idinfo = verify_google_token(secure_token, google_client_id)
                if not secure_token or idinfo is None:
                    return jsonify({'error': 'Unauthorized'}), 401  # Return a JSON response with 401 status
            elif auth_type == 'email_login':
                if not user_id or not verify_email_token(user_id, secure_token):
                    return jsonify({'error': 'Unauthorized'}), 401  # Return a JSON response with 401 status

            response_data = {'data': secure_data}
            response = make_response(jsonify(response_data), 200)
            response.headers["Content-Type"] = "application/json"

            return response

        except Exception as e:
            error_message = f"[ERROR::{invoking_function_name}] Error: {e}"
            self.insert_log(invoking_function_name, error_message, sqlite_db_path, telegram_bot_preset_name)
            self.send_telegram_message(invoking_function_name, error_message, telegram_bot_preset_name)
            return jsonify({"error": error_message}), 500

    def is_user_valid(self, invoking_function_name, request, sqlite_db_path, google_client_id, telegram_bot_preset_name):

        def verify_google_token(token, google_client_id):
            try:
                idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), google_client_id)
                return idinfo is not None  # Return True if the token is valid
            except ValueError:
                return False

        def verify_email_token(user_id, token):
            try:
                # self.send_telegram_message(invoking_function_name, f"Debug: user_id: {user_id} \n\n token: {token}", telegram_bot_preset_name)
                conn = sqlite3.connect(sqlite_db_path)
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT active_token
                       FROM user
                       WHERE id = ?""",
                    (user_id,)
                )
                record = cursor.fetchone()
                # self.send_telegram_message(invoking_function_name, f"Debug: record[0]: {record[0]}", telegram_bot_preset_name)
                if record and record[0] == token:
                    return True
                return False
            except Exception as e:
                self.insert_log(invoking_function_name, f"Database error: {e}", sqlite_db_path, telegram_bot_preset_name)
                self.send_telegram_message(invoking_function_name, f"Database error: {e}", telegram_bot_preset_name)
                return False
            finally:
                conn.close()

        try:
            auth_header = request.headers.get('Authorization')
            auth_type = request.headers.get('AuthTokenType')
            user_id = request.headers.get('UserId')
            
            if auth_header and auth_header.startswith('Bearer '):
                secure_token = auth_header.split(' ')[1]  # Extract the token part
            else:
                secure_token = None

            if auth_type == 'google_auth':
                if secure_token and verify_google_token(secure_token, google_client_id):
                    return True
            elif auth_type == 'email_login':
                if user_id and verify_email_token(user_id, secure_token):
                    return True

            return False

        except Exception as e:
            error_message = f"[ERROR::{invoking_function_name}] Error: {e}"
            self.insert_log(invoking_function_name, error_message, sqlite_db_path, telegram_bot_preset_name)
            self.send_telegram_message(invoking_function_name, error_message, telegram_bot_preset_name)
            return jsonify({"error": error_message}), 500

