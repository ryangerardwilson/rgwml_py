import pymysql
import subprocess
import os
import time
import json

def create_database(config):
    temp_config = config.copy()
    temp_config.pop('database', None)  # Remove the database key to connect to the server

    conn = pymysql.connect(**temp_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
    cursor.close()
    conn.close()

def create_user_table(config, modal_backend_config):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    
    # Create the users table if it does not exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        username TEXT,
        password TEXT,
        type TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS users_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        operation_type ENUM('CREATE', 'UPDATE', 'DELETE'),
        operation_details JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)

    # Check if a 'sudo' user already exists
    cursor.execute("SELECT COUNT(*) FROM users WHERE type = 'sudo'")
    result = cursor.fetchone()
    sudo_user_exists = result[0] > 0
    
    # Seed the database with a 'sudo' user if one does not exist
    if not sudo_user_exists:
        sudo_user = modal_backend_config.get('sudo')
        if sudo_user:
            user_id = 1
            username = sudo_user.get('username')
            password = sudo_user.get('password')
            cursor.execute("""
            INSERT INTO users (user_id, username, password, type)
            VALUES (%s, %s, %s, 'sudo')
            """, (user_id, username, password))
    
    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def create_modal_table(config, modal_name, columns):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    columns_definition = ", ".join([f"{col} TEXT" for col in columns])
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {modal_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        {columns_definition},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {modal_name}_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        operation_type ENUM('CREATE', 'UPDATE', 'DELETE'),
        operation_details JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)
    cursor.close()
    conn.close()

def create_bottle_app(modal_map, config, deploy_port):
    app_code = f"""
import pymysql
import json
from bottle import Bottle, request, response, run, error, HTTPResponse
from datetime import datetime

config = {config}

modal_map = {modal_map}

app = Bottle()

@app.hook('before_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'
    if request.method == 'OPTIONS':
        return HTTPResponse(status=200)

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

@app.error(500)
def error500(error):
    return "Internal Server Error"

@app.route('/favicon.ico')
def serve_favicon():
    return {{"status":"success"}}

@app.route('/authenticate', method=['POST', 'OPTIONS'])
def authenticate():
    if request.method == 'OPTIONS':
        return {{}}

    data = request.json
    username = data.get('username')
    password = data.get('password')

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT id, username, password, type FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and user['password'] == password:
            return {{"status": "success", "message": "Authentication successful", "user": user}}
        else:
            return {{"status": "error", "message": "Invalid username or password"}}
    except pymysql.MySQLError as e:
        return {{"status": "error", "message": str(e)}}

@app.route('/create/<modal_name>', method=['OPTIONS', 'POST'])
def create_entry(modal_name):
    if request.method == 'OPTIONS':
        return {{}}

    data = request.json
    columns = modal_map[modal_name].split(",")
    column_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    values = [data.get(col) for col in columns]

    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {{modal_name}} (user_id, {{column_names}}) VALUES (%s, {{placeholders}})", [data['user_id']] + values)
    conn.commit()
    cursor.close()
    conn.close()

    log_operation(modal_name, data['user_id'], 'CREATE', {{"user_id": data['user_id'], **data}})
    return {{"status": "success"}}

@app.route('/read/<modal_name>', method=['OPTIONS', 'GET'])
def read_entries(modal_name):
    if request.method == 'OPTIONS':
        return {{}}

    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    if modal_name != 'favicon':
        print(f"SELECT * FROM {{modal_name}}")
        cursor.execute(f"SELECT * FROM {{modal_name}}")
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        # Serialize datetime objects
        serialized_result = []
        for row in result:
            serialized_row = []
            for item in row:
                if isinstance(item, datetime):
                    serialized_row.append(item.isoformat())
                else:
                    serialized_row.append(item)
            serialized_result.append(serialized_row)

        return {{"columns": column_names, "data": serialized_result}}

@app.route('/update/<modal_name>/<entry_id>', method=['PUT','OPTIONS'])
def update_entry(modal_name, entry_id):

    if request.method == 'OPTIONS':
        return {{}}

    data = request.json
    columns = modal_map[modal_name].split(",")
    
    # Remove columns that should not be updated
    columns = [col for col in columns if col not in ['id', 'created_at', 'updated_at', 'user_id']]

    # Construct the SET clause with placeholders for SQL parameters
    set_clause = ", ".join([f"{{col}} = %s" for col in columns])
    values = [data.get(col) for col in columns]

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    try:
        # Fetch the old row for logging purposes
        cursor.execute(f"SELECT * FROM {{modal_name}} WHERE id = %s", [entry_id])
        old_row = cursor.fetchone()
        print(old_row)

        # Perform the update operation
        cursor.execute(f"UPDATE {{modal_name}} SET {{set_clause}} WHERE id = %s", values + [entry_id])
        conn.commit()

        # Log the operation
        log_operation(modal_name, data['user_id'], 'UPDATE', {{"old": old_row, "new": data}})

        return {{"status": "success"}}
    except Exception as e:
        conn.rollback()
        return {{"status": "error", "message": str(e)}}
    finally:
        cursor.close()
        conn.close()

@app.route('/delete/<modal_name>/<entry_id>', method=['OPTIONS', 'DELETE'])
def delete_entry(modal_name, entry_id):

    if request.method == 'OPTIONS':
        return {{}}

    data = request.json
    user_id = data.get('user_id')
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {{modal_name}} WHERE id = %s", [entry_id])
    old_row = cursor.fetchone()
    cursor.execute(f"DELETE FROM {{modal_name}} WHERE id = %s", [entry_id])
    conn.commit()
    cursor.close()
    log_operation(modal_name, user_id, 'DELETE', {{ "deleted_row": old_row }})
    return {{"status": "success"}}

def log_operation(modal_name, user_id, operation_type, operation_details):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {{modal_name}}_logs (user_id, operation_type, operation_details) VALUES (%s, %s, %s)", [user_id, operation_type, json.dumps(operation_details, default=serialize_datetime)])
    conn.commit()
    cursor.close()
    conn.close()

run(app, host='0.0.0.0', port={deploy_port})
"""
    with open("app.py", "w") as f:
        f.write(app_code)

def kill_process_on_port(ssh_key_path, gcs_instance, port):
    kill_cmd = f"fuser -k {port}/tcp || true"
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, kill_cmd])

def deploy_to_gcs(ssh_key_path, gcs_instance, deploy_path):
    # Create the deployment directory on the remote server
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'mkdir -p {deploy_path}'])
    
    # Copy the app.py file to the remote server
    subprocess.run(['scp', '-i', ssh_key_path, 'app.py', f'{gcs_instance}:{deploy_path}/'])
    
    # Start the application on the remote server using nohup and detach from the SSH session
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'nohup bash -c "python3 {deploy_path}/app.py > {deploy_path}/app.log 2>&1 &"'])
    
    # Wait for the server to start
    time.sleep(5)

def run_tests(base_url, modal_map):
    for modal, columns in modal_map.items():
        columns_list = columns.split(',')
        test_data = {col: "xxxxx" for col in columns_list}
        test_data['user_id'] = 1

        if modal != 'users':
            print(f"\nTesting {modal} modal:\n")

            # Create entry
            create_response = subprocess.run([
                'curl', '-X', 'POST', f"{base_url}/create/{modal}",
                '-H', "Content-Type: application/json",
                '-d', json.dumps(test_data)
            ], capture_output=True, text=True)
            print("Create Entry Response:", create_response.stdout)

            # Read entries
            read_response = subprocess.run(['curl', '-X', 'GET', f"{base_url}/read/{modal}"], capture_output=True, text=True)
            print("Read Entries Response:", read_response.stdout)

            # Update entry
            updated_data = {col: "xxxxx_updated" for col in columns_list}
            updated_data['user_id'] = 1
            update_response = subprocess.run([
                'curl', '-X', 'PUT', f"{base_url}/update/{modal}/1",
                '-H', "Content-Type: application/json",
                '-d', json.dumps(updated_data)
            ], capture_output=True, text=True)
            print("Update Entry Response:", update_response.stdout)

            # Delete entry
            delete_response = subprocess.run([
                'curl', '-X', 'DELETE', f"{base_url}/delete/{modal}/1",
                '-H', "Content-Type: application/json",
                '-d', json.dumps({'user_id': 1})
            ], capture_output=True, text=True)
            print("Delete Entry Response:", delete_response.stdout)

def cleanup_db(config, modal_map):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    for modal_name, columns in modal_map.items():
        if modal_name != 'users':
            columns_list = columns.split(',')
            conditions = " OR ".join([f"{col} LIKE '%%xxxxx%%'" for col in columns_list])
            
            # Delete from modal table
            query = f"DELETE FROM {modal_name} WHERE {conditions}"
            cursor.execute(query)
            
            # Delete from modal logs table
            log_conditions = " OR ".join([f"operation_details LIKE '%%{col}%%xxxxx%%'" for col in columns_list])
            log_query = f"DELETE FROM {modal_name}_logs WHERE {log_conditions}"
            cursor.execute(log_query)
            
            conn.commit()
    cursor.close()
    conn.close()


def main(config, modal_backend_config, ssh_key_path, instance, deploy_at, deploy_port):
    create_database(config)
    create_user_table(config, modal_backend_config)

    modal_map = modal_backend_config["modals"]

    
    for modal_name, columns in modal_map.items():
        create_modal_table(config, modal_name, columns.split(','))

    modal_map["users"] = "username,password,type"
    create_bottle_app(modal_map, config, deploy_port)

    # Ensure the port is cleared on the remote server
    kill_process_on_port(ssh_key_path, instance, deploy_port)

    deploy_to_gcs(ssh_key_path, instance, deploy_at)

    # Run dynamic tests
    run_tests(f"http://{config['host']}:{deploy_port}", modal_map)

    cleanup_db(config, modal_map)
    print(f"Backend deployed to: http://{config['host']}:{deploy_port}")
    


