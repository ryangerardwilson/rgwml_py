import os
import json
import inspect
import sqlite3
import requests
import time
import re
import secrets
from datetime import datetime
from flask import request, jsonify, make_response
from google.oauth2 import id_token, service_account
from google.auth.transport import requests as google_requests
from googleapiclient.discovery import build
import base64


class m:

    def google_auth(self, invoking_function_name, request, google_client_id, sqlite_db_path, post_authentication_redirect_url,
                    telegram_bot_preset_name, cookie_domain):

        def verify_token(token, google_client_id):
            try:
                idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), google_client_id)
                return idinfo
            except ValueError:
                return None

        def create_tables(sqlite_db_path):
            conn = sqlite3.connect(sqlite_db_path)
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        google_auth_user_id TEXT UNIQUE,
                        email TEXT,
                        name TEXT,
                        picture TEXT
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_login_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES user (id)
                    )
                ''')
                conn.commit()
            finally:
                conn.close()

        request_body_json = request.json
        token = request_body_json.get('token')
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

                    create_tables(sqlite_db_path)

                    conn = sqlite3.connect(sqlite_db_path)
                    self.send_telegram_message(invoking_function_name, "STEP2", telegram_bot_preset_name)

                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            "INSERT INTO user (google_auth_user_id, email, name, picture) VALUES (?, ?, ?, ?) "
                            "ON CONFLICT(google_auth_user_id) DO UPDATE SET email=excluded.email, name=excluded.name, picture=excluded.picture",
                            (google_auth_user_id, email, name, picture))
                        self.send_telegram_message(invoking_function_name, "STEP3", telegram_bot_preset_name)

                        cursor.execute("SELECT id, email FROM user WHERE google_auth_user_id = ?", (google_auth_user_id,))
                        result = cursor.fetchone()
                        id_in_user_table = result[0]
                        email_in_user_table = result[1]

                        self.send_telegram_message(invoking_function_name, "STEP4", telegram_bot_preset_name)

                        cursor.execute("INSERT INTO user_login_logs (user_id) VALUES (?)", (id_in_user_table,))

                        self.send_telegram_message(invoking_function_name, "STEP5", telegram_bot_preset_name)
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        raise e
                    finally:
                        conn.close()

                    response_data = {
                        'success': True,
                        'user': idinfo,
                        'id_in_user_table': id_in_user_table,
                        'email_in_user_table': email_in_user_table,
                        'url': post_authentication_redirect_url
                    }
                    response = make_response(jsonify(response_data), 200)
                    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
                    response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
                    response.headers.add('Content-Type', 'text/html; charset=UTF-8')
                    response.headers.add('Set-Cookie', f"authToken={token}; Domain={cookie_domain}; HttpOnly; Path=/; SameSite=None; Secure")
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

            credentials_path = preset.get("credentials_path")
            gmail_id = preset.get("gmail_id")
            credentials_type = preset.get("credentials_type")

            if not credentials_path or not gmail_id or not credentials_type:
                raise RuntimeError(
                    f"Gmail credentials_path, gmail_id or credentials_type for '{preset_name}' not found in the configuration file. \n\n{credentials_path} \n\n {gmail_id} \n\n {credentials_type}"
                )
            return credentials_path, gmail_id, credentials_type

        def is_valid_email(email):
            regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
            return re.match(regex, email)

        def create_tables_if_not_exist(cursor):
            cursor.execute('''CREATE TABLE IF NOT EXISTS user (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                google_auth_user_id TEXT UNIQUE,
                                email TEXT UNIQUE,
                                name TEXT,
                                picture TEXT
                              )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_pending_validation (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                email TEXT UNIQUE,
                                temp_password TEXT,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              )''')

        config = load_config()
        credentials_path, gmail_id, credentials_type = get_gmail_bot_details(config, gmail_bot_preset_name)

        request_body_json = request.json
        email = request_body_json.get('email')

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

            if user:
                return jsonify(success=True, message="now_confirm_password"), 200

            # Store the email and temp password in the user_pending_validation table
            cursor.execute(
                """INSERT OR IGNORE INTO user_pending_validation (email)
                   VALUES (?)""",
                (email,)
            )
            conn.commit()

            return jsonify(success=True, message="now_set_password"), 200

        except Exception as e:
            self.send_telegram_message(invoking_function_name, f"111 error: {e}", telegram_bot_preset_name)
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

    def send_secure_data(self, invoking_function_name, secure_data, sqlite_db_path, telegram_bot_preset_name, google_client_id):

        def verify_token(token, google_client_id):
            try:
                idinfo = id_token.verify_oauth2_token(token, google_requests.Request(), google_client_id)
                return idinfo  # Return user profile information
            except ValueError:
                return None

        try:
            # secure_token = request.cookies.get('authToken')  # Get the token from cookies
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith('Bearer '):
                secure_token = auth_header.split(' ')[1]  # Extract the token part
            else:
                secure_token = None

            idinfo = verify_token(secure_token, google_client_id)
            if not secure_token or idinfo is None:
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
