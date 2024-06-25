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
        username VARCHAR(255),
        password VARCHAR(255),
        type VARCHAR(255),
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
    columns_definition = ", ".join([f"{col} {col_type}" for col, col_type in columns])
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

@app.route('/read/<modal_name>/<route_name>', method=['OPTIONS', 'GET'])
def read_entries(modal_name, route_name):
    if request.method == 'OPTIONS':
        return {{}}

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        if modal_name != 'favicon':
            query = None
            for route in modal_map[modal_name]['read_routes']:
                if route_name in route:
                    query = route[route_name]
                    break
            if not query:
                raise ValueError(f"Read route '{{route_name}}' not found for modal '{{modal_name}}'")
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



@app.route('/query/<modal_name>', method=['OPTIONS', 'POST'])
def query_entries(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Allow'] = 'OPTIONS, POST'
        return {{}}

    data = request.json
    if not data or 'query_string' not in data:
        response.status = 400
        return {{"error": "query_string is required in the request body"}}

    query_string = data['query_string'].strip()
    lower_query_string = query_string.lower()
    expected_start = f"select * from {{modal_name}}".lower()

    if not lower_query_string.startswith(expected_start):
        response.status = 400
        return {{"error": f"query_string must start with 'SELECT * FROM {{modal_name}}'"}}

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        cursor.execute(query_string)
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



@app.route('/search/<modal_name>', method=['OPTIONS', 'POST'])
def search_entries(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Allow'] = 'OPTIONS, POST'
        return {{}}

    if request.method == 'POST':
        data = request.json
        if not data or 'search_string' not in data:
            response.status = 400
            return {{"error": "search_string is required in the request body"}}

        search_string = data['search_string']
        try:
            conn = pymysql.connect(**config)
            cursor = conn.cursor()

            # Retrieve column names of the table
            cursor.execute(f"SHOW COLUMNS FROM {{modal_name}}")
            columns = [row[0] for row in cursor.fetchall()]

            # Construct the WHERE clause to search across all columns
            where_clause = " OR ".join([f"{{col}} LIKE '%{{search_string}}%'" for col in columns])
            query = f"SELECT * FROM {{modal_name}} WHERE {{where_clause}}"

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

def parse_column_names(columns_str):
    columns = []
    for col in columns_str.split(','):
        if '[' in col and ']' in col:
            col_name = col.split('[')[0]
        else:
            col_name = col
        columns.append(col_name.strip())
    return columns

def run_tests(base_url, modal_map):
    for modal, details in modal_map.items():
        columns_str = details['columns']
        read_routes = details.get('read_routes', [])

        # Parse columns to get names only
        columns_list = parse_column_names(columns_str)
        print(f"Parsed columns for modal {modal}: {columns_list}")

        # Prepare test data
        test_data = {col: "xxxxx" if col != 'user_id' else 1 for col in columns_list}
        print(f"Test data for modal {modal}: {test_data}")

        print(f"\nTesting {modal} modal:\n")

        # Create entry
        create_response = subprocess.run([
            'curl', '-X', 'POST', f"{base_url}/create/{modal}",
            '-H', "Content-Type: application/json",
            '-d', json.dumps(test_data)
        ], capture_output=True, text=True)
        print("Create Entry Response:", create_response.stdout)

        # Read entries
        for route in read_routes:
            route_name = list(route.keys())[0]
            read_response = subprocess.run(['curl', '-X', 'GET', f"{base_url}/read/{modal}/{route_name}"], capture_output=True, text=True)
            print(f"Read Entries Response for {route_name}:", read_response.stdout)

        # Query entries
        query_data = {"query_string": f"SELECT * FROM {modal} WHERE user_id = 1 ORDER BY id DESC"}
        print(f"Query data for modal {modal}: {query_data}")
        query_response = subprocess.run([
            'curl', '-X', 'POST', f"{base_url}/query/{modal}",
            '-H', "Content-Type: application/json",
            '-d', json.dumps(query_data)
        ], capture_output=True, text=True)
        print("Query Entries Response:", query_response.stdout)
        print("Query Entries Response (stderr):", query_response.stderr)

        # Search entries
        search_conditions = " OR ".join([f"{col} LIKE '%%xxxxx%%'" for col in columns_list if col != 'user_id'])
        search_data = {"query_string": f"SELECT * FROM {modal} WHERE {search_conditions}"}
        print(f"Search data for modal {modal}: {search_data}")
        search_response = subprocess.run([
            'curl', '-X', 'POST', f"{base_url}/search/{modal}",
            '-H', "Content-Type: application/json",
            '-d', json.dumps(search_data)
        ], capture_output=True, text=True)
        print("Search Entries Response:", search_response.stdout)

        # Update entry
        updated_data = {col: "xxxxx_updated" if col != 'user_id' else 1 for col in columns_list}
        print(f"Updated data for modal {modal}: {updated_data}")
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
    for modal_name, details in modal_map.items():
        if modal_name == 'users':
            continue

        columns_str = details['columns']
        columns_list = parse_column_names(columns_str)
        print(f"Parsed columns for modal {modal_name}: {columns_list}")

        conditions = " OR ".join([f"{col} LIKE '%%xxxxx%%'" for col in columns_list])
        print(f"Conditions for modal {modal_name}: {conditions}")

        # Delete from modal table
        query = f"DELETE FROM {modal_name} WHERE {conditions}"
        print(f"Delete query for modal {modal_name}: {query}")
        cursor.execute(query)

        # Delete from modal logs table
        log_conditions = " OR ".join([f"operation_details LIKE '%%{col}%%xxxxx%%'" for col in columns_list])
        log_query = f"DELETE FROM {modal_name}_logs WHERE {log_conditions}"
        print(f"Delete log query for modal {modal_name}: {log_query}")
        cursor.execute(log_query)

        conn.commit()
    cursor.close()
    conn.close()


def parse_column_definitions(columns_str):
    columns = []
    for col in columns_str.split(','):
        if '[' in col and ']' in col:
            col_name, col_type = col.split('[')
            col_type = col_type.rstrip(']')
        else:
            col_name, col_type = col, 'VARCHAR(255)'
        columns.append((col_name.strip(), col_type.strip()))
    return columns

def main(project_name, new_db_name, db_config, modal_backend_config, ssh_key_path, instance, backend_vm_deploy_path, backend_domain, netlify_key, vm_preset, vm_host):
    create_database(db_config, new_db_name)
    create_user_table(db_config, modal_backend_config)

    modal_map = modal_backend_config["modals"]

    for modal_name, modal_details in modal_map.items():
        columns = parse_column_definitions(modal_details["columns"])
        create_modal_table(db_config, modal_name, columns)

    #modal_map["users"] = "username,password,type"

    modal_map['users'] = {
        "columns": "username,password,type",
        "read_routes": [
            {"default": "SELECT * FROM users"}
        ]
    }


    create_bottle_app(modal_map, db_config)

    direct_domain_to_instance(netlify_key, project_name, backend_domain, vm_host)
    upload_to_gcs(ssh_key_path, instance, backend_vm_deploy_path)
    deploy_to_gcs(vm_preset, backend_vm_deploy_path, project_name, new_db_name, backend_domain)

    # Run dynamic tests
    run_tests(f"https://{backend_domain}", modal_map)

    cleanup_db(db_config, modal_map)
    print(f"Backend deployed to: https://{backend_domain}")
    


