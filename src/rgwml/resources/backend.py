import requests
import pymysql
import subprocess
import os
import time
import json
import paramiko
import time

def create_database(config, new_db_name):
    temp_config = config.copy()
    temp_config.pop('database', None)  # Remove the database key to connect to the server

    conn = pymysql.connect(**temp_config)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_db_name}")
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

def create_bottle_app(modal_map, config):
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
        cursor.execute(f"SELECT * FROM {{modal_name}} ORDER BY id DESC LIMIT 1000")
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

@app.route('/query/<modal_name>', method=['OPTIONS', 'POST'])
def query_entries(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Allow'] = 'OPTIONS, POST'
        return {{}}

    if request.method == 'POST':
        data = request.json
        if not data or 'query_string' not in data:
            response.status = 400
            return {{"error": "query_string is required in the request body"}}

        query_string = data['query_string']
        query = f"SELECT * FROM {{modal_name}} WHERE {{query_string}} LIMIT 1000"

        try:
            conn = pymysql.connect(**config)
            cursor = conn.cursor()
            cursor.execute(query)
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
        except Exception as e:
            response.status = 500
            return {{"error": str(e)}}

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

"""
    with open("app.py", "w") as f:
        f.write(app_code)

def kill_process_on_port(ssh_key_path, gcs_instance, port):
    kill_cmd = f"fuser -k {port}/tcp || true"
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, kill_cmd])

















"""
def deploy_to_gcs(ssh_key_path, gcs_instance, deploy_path):
    # Create the deployment directory on the remote server
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'mkdir -p {deploy_path}'])
    
    # Copy the app.py file to the remote server
    subprocess.run(['scp', '-i', ssh_key_path, 'app.py', f'{gcs_instance}:{deploy_path}/'])
    
    # Start the application on the remote server using nohup and detach from the SSH session
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'nohup bash -c "python3 {deploy_path}/app.py > {deploy_path}/app.log 2>&1 &"'])
    
    # Wait for the server to start
    time.sleep(5)
"""

def direct_domain_to_instance(netlify_key, project_name, backend_domain, vm_host):

    def get_dns_zone_id(domain):
        url = "https://api.netlify.com/api/v1/dns_zones"
        headers = {"Authorization": f"Bearer {netlify_key}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        zones = response.json()
        print(f"DNS zones retrieved: {zones}")  # Debugging line
        for zone in zones:
            if zone["name"] == domain:
                return zone["id"]
        return None

    def create_dns_zone(domain):
        url = "https://api.netlify.com/api/v1/dns_zones"
        headers = {
            "Authorization": f"Bearer {netlify_key}",
            "Content-Type": "application/json"
        }
        data = {
            "name": domain
        }
        response = requests.post(url, headers=headers, json=data)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 422 and "conflicting zone" in response.json().get("errors", {}).get("name", [])[0]:
                print(f"DNS zone for {domain} already exists. Skipping creation.")
                return get_dns_zone_id(domain)
            else:
                print(f"Error creating DNS zone: {e}")
                print(response.content)
                raise
        return response.json()["id"]

    def get_dns_records(zone_id):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records"
        headers = {"Authorization": f"Bearer {netlify_key}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def create_dns_record(zone_id, hostname, ip_address):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records"
        headers = {
            "Authorization": f"Bearer {netlify_key}",
            "Content-Type": "application/json"
        }
        data = {
            "type": "A",
            "hostname": hostname,
            "value": ip_address,
            "ttl": 3600
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    parent_domain = ".".join(backend_domain.split('.')[-2:])
    subdomain = backend_domain.replace(f".{parent_domain}", "")

    zone_id = get_dns_zone_id(parent_domain)
    print(f"Initial DNS zone ID: {zone_id}")  # Debugging line
    if zone_id is None:
        zone_id = create_dns_zone(parent_domain)
        print(f"DNS zone ID after creation attempt: {zone_id}")  # Debugging line
        if zone_id is None:
            print("Retrying to fetch DNS zone ID after creation attempt.")
            time.sleep(5)  # Adding a short delay before retrying
            zone_id = get_dns_zone_id(parent_domain)
            print(f"Fetched DNS zone ID after retry: {zone_id}")  # Debugging line
        if zone_id is None:
            raise ValueError(f"Unable to retrieve DNS zone ID for {parent_domain} after creation attempt.")
        else:
            print(f"Created DNS zone for {parent_domain} with ID: {zone_id}")

    dns_records = get_dns_records(zone_id)
    print(f"Retrieved DNS records for zone ID {zone_id}: {dns_records}")

    exists = False
    for record in dns_records:
        if record["hostname"] == subdomain and record["value"] == vm_host:
            exists = True
            break
    if not exists:
        create_dns_record(zone_id, subdomain, vm_host)
        print(f"Created DNS record for {subdomain} pointing to {vm_host}")
    else:
        print(f"DNS record for {subdomain} already exists and points to {vm_host}")


def upload_to_gcs(ssh_key_path, gcs_instance, deploy_path):
    # Create the deployment directory on the remote server
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'mkdir -p {deploy_path}'])
            
    # Copy the app.py file to the remote server
    subprocess.run(['scp', '-i', ssh_key_path, 'app.py', f'{gcs_instance}:{deploy_path}/'])
    
    # Wait for the server to start
    time.sleep(3)



def deploy_to_gcs(vm_preset, backend_vm_deploy_path, project_name, new_db_name, backend_domain):

    location_of_app_file_on_vm = f"{backend_vm_deploy_path}/app.py"
    app_name = project_name
    app_file_name = "app"
    domain_name = backend_domain

    app_directory = os.path.dirname(location_of_app_file_on_vm)
    service_file_name = f"{new_db_name}_API_BACKEND_SYSTEMD_SERVICE_FILE"
    nginx_config_file_name = f"{domain_name}"
    service_file_path = os.path.join(app_directory, f"{service_file_name}.service")
    temp_nginx_config_file_path = os.path.join(app_directory, f"temp_{nginx_config_file_name}")

    commands = [
        "sudo apt update",
        "sudo apt install -y python3-pip nginx",
        "sudo pip3 uninstall -y gunicorn bottle",
        "sudo pip3 install gunicorn bottle pymysql",
        f"echo '[Unit]\nDescription=Gunicorn instance to serve {new_db_name} API\nAfter=network.target\n\n[Service]\nWorkingDirectory={app_directory}\nExecStart=/usr/local/bin/gunicorn --workers=4 --bind unix:{app_directory}/{app_file_name}.sock app:app\nEnvironment=\"PATH=/usr/local/bin:/usr/bin:/bin\"\nEnvironment=\"PYTHONPATH=/usr/local/lib/python3.10/dist-packages\"\nRestart=always\n\n[Install]\nWantedBy=multi-user.target' > {service_file_path}",
        f"sudo mv {service_file_path} /etc/systemd/system/{service_file_name}.service",
        "sudo systemctl daemon-reload",
        f"sudo systemctl restart {service_file_name}",
        f"sudo systemctl status {service_file_name}",
        f"echo 'server {{\nlisten 80;\nserver_name {domain_name};\n\nlocation / {{\ninclude proxy_params;\nproxy_pass http://unix:{app_directory}/{app_file_name}.sock;\n}}\n}}' > {temp_nginx_config_file_path}",
        f"sudo mv {temp_nginx_config_file_path} /etc/nginx/sites-available/{nginx_config_file_name}",
        f"sudo chmod 777 /etc/nginx/sites-available/{nginx_config_file_name}",
        f"if [ -L /etc/nginx/sites-enabled/{domain_name} ]; then sudo rm /etc/nginx/sites-enabled/{domain_name}; fi",
        f"sudo ln -s /etc/nginx/sites-available/{nginx_config_file_name} /etc/nginx/sites-enabled/{domain_name}",
        "sudo nginx -t",
        "sudo systemctl reload nginx",
        "sudo ufw allow 'Nginx Full'",
        "sudo apt install -y certbot python3-certbot-nginx",
        "for pid in $(pgrep certbot); do sudo kill -9 $pid; done",
        "while pgrep certbot > /dev/null; do sleep 1; done",
        f"sudo certbot --nginx -d {domain_name} --reinstall",
    ]

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(vm_preset['host'], username=vm_preset['ssh_user'], key_filename=vm_preset['ssh_key_path'])

    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        print(stdout.read().decode())
        print(stderr.read().decode())

    ssh.close()



















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

            # Query entries
            query_data = {"query_string": "user_id = 1 ORDER BY id DESC"}
            query_response = subprocess.run([
                'curl', '-X', 'POST', f"{base_url}/query/{modal}",
                '-H', "Content-Type: application/json",
                '-d', json.dumps(query_data)
            ], capture_output=True, text=True)
            print("Query Entries Response:", query_response.stdout)

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


def main(project_name, new_db_name, db_config, modal_backend_config, ssh_key_path, instance, backend_vm_deploy_path, backend_domain, netlify_key, vm_preset, vm_host):
    create_database(db_config, new_db_name)
    create_user_table(db_config, modal_backend_config)

    modal_map = modal_backend_config["modals"]

    
    for modal_name, columns in modal_map.items():
        create_modal_table(db_config, modal_name, columns.split(','))

    modal_map["users"] = "username,password,type"
    create_bottle_app(modal_map, db_config)

    direct_domain_to_instance(netlify_key, project_name, backend_domain, vm_host)
    upload_to_gcs(ssh_key_path, instance, backend_vm_deploy_path)
    deploy_to_gcs(vm_preset, backend_vm_deploy_path, project_name, new_db_name, backend_domain)

    # Run dynamic tests
    run_tests(f"https://{backend_domain}", modal_map)

    cleanup_db(db_config, modal_map)
    print(f"Backend deployed to: https://{backend_domain}")
    


