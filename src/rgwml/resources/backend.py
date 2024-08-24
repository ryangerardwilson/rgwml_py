import requests
import pymysql
import subprocess
import os
import re
import time
import json
import paramiko


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

    cursor.execute("""
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

    # Function to clean the column names
    def clean_columns(columns_str):
        # Remove data type references in square brackets
        clean_columns = re.sub(r'\[.*?\]', '', columns_str)
        # Remove extra whitespace and return the cleaned columns string
        return clean_columns.replace(" ", "")

    # Clean the columns in the modal_map
    for modal, details in modal_map.items():
        columns_str = details.get('columns', '')
        clean_columns_str = clean_columns(columns_str)
        modal_map[modal]['columns'] = clean_columns_str

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
    if not data or 'user_id' not in data:
        response.status = 400
        return {{"error": "user_id is required in the request body"}}
    try:
        modal_details = modal_map.get(modal_name, {{}})
        if not modal_details:
            raise ValueError(f"Modal {{modal_name}} not found in modal_map")

        columns_str = modal_details.get('columns', '')
        columns = columns_str.split(",")

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
    except Exception as e:
        print(f"Error creating entry: {{e}}")
        response.status = 500
        return {{"error": str(e)}}

@app.route('/bulk_create/<modal_name>', method=['OPTIONS', 'POST'])
def create_entry(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Content-Type'] = 'application/json'
        return {{}}

    data = request.json
    if not data or 'user_id' not in data:
        response.status = 400
        response.content_type = 'application/json'
        return {{"error": "user_id is required in the request body"}}

    try:
        modal_details = modal_map.get(modal_name, {{}})
        if not modal_details:
            raise ValueError(f"Modal {{modal_name}} not found in modal_map")

        if 'columns' not in data or 'data' not in data:
            raise ValueError("Both 'columns' and 'data' fields are required in the request body")

        user_id = data['user_id']
        columns = data['columns']
        bulk_values = data['data']

        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValueError("'columns' should be a list of strings")
        if not isinstance(bulk_values, list) or not all(isinstance(val, list) for val in bulk_values):
            raise ValueError("'data' should be a list of lists")

        column_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        values_list = [[user_id] + entry for entry in bulk_values]

        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        insert_query = f"INSERT INTO {{modal_name}} (user_id, {{column_names}}) VALUES (%s, {{placeholders}})"
        cursor.executemany(insert_query, values_list)
        conn.commit()
        cursor.close()
        conn.close()


        log_entries = [
            {{
                'user_id': data['user_id'],
                'operation_type': 'CREATE',
                'operation_details': {{**dict(zip(columns, entry))}}
            }}
            for entry in bulk_values
        ]
        bulk_log_operation(modal_name, log_entries)

        response.content_type = 'application/json'
        return {{"status": "success"}}

    except Exception as e:
        print(f"Error creating entry: {{e}}")
        response.status = 500
        response.content_type = 'application/json'
        return {{"error": str(e)}}

@app.route('/read/<modal_name>/<route_name>', method=['OPTIONS', 'GET'])
@app.route('/read/<modal_name>/<route_name>/<belongs_to_user_id>', method=['OPTIONS', 'GET'])
def read_entries(modal_name, route_name, belongs_to_user_id=None):
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

            if belongs_to_user_id:
                query = query.replace('$belongs_to_user_id$', belongs_to_user_id)

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

@app.route('/bulk_read/<modal_name>/<limit_name>', method=['OPTIONS', 'GET'])
def bulk_read_entries(modal_name, limit_name):
    if request.method == 'OPTIONS':
        return {{}}

    limit_mappings = {{
        'today': 'WHERE DATE(created_at) = CURDATE()',
        'since_yesterday': 'WHERE DATE(created_at) >= CURDATE() - INTERVAL 1 DAY',
        'since_last_7_days': 'WHERE DATE(created_at) >= CURDATE() - INTERVAL 7 DAY',
        'since_last_14_days': 'WHERE DATE(created_at) >= CURDATE() - INTERVAL 14 DAY',
        'since_last_28_days': 'WHERE DATE(created_at) >= CURDATE() - INTERVAL 28 DAY',
        'since_last_90_days': 'WHERE DATE(created_at) >= CURDATE() - INTERVAL 90 DAY',
    }}

    try:
        limit_clause = limit_mappings.get(limit_name, '')
        if not limit_clause:
            response.status = 400
            return {{"error": "Invalid limit_name"}}

        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        if modal_name != 'favicon':

            query = f"SELECT * FROM {{modal_name}} {{limit_clause}}"

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

@app.route('/update/<modal_name>/<entry_id>', method=['PUT', 'OPTIONS'])
def update_entry(modal_name, entry_id):
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'PUT, OPTIONS'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return {{}}

    data = request.json
    modal_details = modal_map.get(modal_name, {{}})
    if not modal_details:
        response.status = 400
        return {{"error": f"Modal {{modal_name}} not found in modal_map"}}

    columns_str = modal_details.get('columns', '')
    columns = [col for col in columns_str.split(",") if col not in ['id', 'created_at', 'updated_at']]

    set_clauses = []
    params = []

    for col in columns:
        if col in data:  # Check if the column is in the data received
            value = data[col]
            if value is not None:
                set_clauses.append(f"`{{col}}`=%s")  # use parameterized queries
                params.append(value)

    # Ensure that user_id is included if present in the data
    if 'user_id' in data:
        value = data['user_id']
        if value is not None:
            set_clauses.append("`user_id`=%s")
            params.append(value)

    set_clause = ", ".join(set_clauses)
    update_query = f"UPDATE `{{modal_name}}` SET {{set_clause}} WHERE id = %s"
    params.append(entry_id)

    conn = None
    cursor = None

    try:
        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        # Fetch the old row for logging purposes
        cursor.execute(f"SELECT * FROM `{{modal_name}}` WHERE id = %s", (entry_id,))
        old_row = cursor.fetchone()
        print(f"Old row: {{old_row}}")

        # Perform the update operation
        cursor.execute(update_query, params)
        conn.commit()

        log_operation(modal_name, data.get('user_id'), 'UPDATE', {{"old": old_row, "new": data}})

        return {{"status": "success"}}
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating entry: {{e}}")
        response.status = 500
        error = str(e)
        message = f"error: {{error}}; query: {{update_query}}"
        return {{"status": "error", "message": message}}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/bulk_update/<modal_name>', method=['OPTIONS', 'PUT'])
def bulk_update(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'PUT, OPTIONS'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return {{}}

    data = request.json
    if not data or 'user_id' not in data:
        response.status = 400
        response.content_type = 'application/json'
        return {{"error": "user_id is required in the request body"}}

    try:
        modal_details = modal_map.get(modal_name, {{}})
        if not modal_details:
            raise ValueError(f"Modal {{modal_name}} not found in modal_map")

        if 'columns' not in data or 'data' not in data:
            raise ValueError("Both 'columns' and 'data' fields are required in the request body")

        user_id = data['user_id']
        columns = data['columns']
        bulk_values = data['data']  # This should be a list of dicts, where each dict contains 'id' and the columns to update

        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValueError("'columns' should be a list of strings")
        if not isinstance(bulk_values, list) or not all(isinstance(entry, dict) for entry in bulk_values):
            raise ValueError("'data' should be a list of dictionaries")

        columns_str = modal_details.get('columns', '')
        valid_columns = [col for col in columns_str.split(",")]
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        entry_ids = [int(entry['id']) for entry in bulk_values]
        # Use placeholders for parameterized queries
        placeholders = ', '.join(['%s'] * len(entry_ids))
        query = f"SELECT * FROM `{{modal_name}}` WHERE id IN ({{placeholders}})"
        # Execute the query with parameterized input
        cursor.execute(query, entry_ids)
        old_rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        # Convert the list of tuples into a list of dictionaries
        result = [{{column_names[i]: row[i] for i in range(len(column_names))}} for row in old_rows]
        # Convert the list of dictionaries to JSON
        #result_json = json.dumps(result, default=lambda x: x.isoformat() if isinstance(x, datetime) else x, indent=4)
        old_rows_dict = {{row['id']: row for row in result}}
        # Prepare batch update statement
        update_entries = []
        update_params = []
        for entry in bulk_values:
            entry_id = entry.get('id')
            if not entry_id:
                raise ValueError("Each entry must have an 'id' field")
            set_clauses = []
            params = []
            for col in columns:
                if col in entry:
                    value = entry[col]
                    if value is not None:
                        set_clauses.append(f"`{{col}}`=%s")
                        params.append(value)
            params.append(entry_id)
            set_clause = ", ".join(set_clauses)
            update_query = f"UPDATE `{{modal_name}}` SET {{set_clause}} WHERE id = %s"
            cursor.execute(update_query, params)
            # Log operation for the specific entry
            log_entry = {{
                'user_id': user_id,
                'operation_type': 'UPDATE',
                'operation_details': {{'old': old_rows_dict.get(entry_id, {{}}), 'new': entry}}
            }}
            bulk_log_operation(modal_name, [log_entry])
        conn.commit()
        response.content_type = 'application/json'
        return {{"status": "success"}}

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating entries: {{e}}")
        response.status = 500
        response.content_type = 'application/json'
        return {{"status": "error", "message": str(e)}}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/delete/<modal_name>/<entry_id>', method=['OPTIONS', 'DELETE'])
def delete_entry(modal_name, entry_id):

    if request.method == 'OPTIONS':
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Origin'] = '*'
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

@app.route('/bulk_delete/<modal_name>', method=['OPTIONS','DELETE'])
def bulk_delete(modal_name):

    if request.method == 'OPTIONS':
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return {{}}

    data = request.json
    if not data or 'user_id' not in data:
        response.status = 400
        response.content_type = 'application/json'
        return {{"error": "user_id is required in the request body"}}

    if 'ids' not in data:
        response.status = 400
        response.content_type = 'application/json'
        return {{"error": "ids field is required in the request body"}}

    try:
        modal_details = modal_map.get(modal_name, {{}})
        if not modal_details:
            raise ValueError(f"Modal {{modal_name}} not found in modal_map")

        user_id = data['user_id']
        ids = data['ids']  # This should be a list of ids to delete

        if not isinstance(ids, list) or not all(isinstance(entry_id, int) for entry_id in ids):
            raise ValueError("'ids' should be a list of integers")

        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        # Use placeholders for parameterized queries
        placeholders = ', '.join(['%s'] * len(ids))
        select_query = f"SELECT * FROM `{{modal_name}}` WHERE id IN ({{placeholders}})"
        cursor.execute(select_query, ids)
        old_rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]

        # Convert the list of tuples into a list of dictionaries
        #old_rows_dict = {{row['id']: {{column_names[i]: row[i] for i in range(len(column_names))}} for row in old_rows}}

        result = [{{column_names[i]: row[i] for i in range(len(column_names))}} for row in old_rows]
        # Convert the list of dictionaries to JSON
        #result_json = json.dumps(result, default=lambda x: x.isoformat() if isinstance(x, datetime) else x, indent=4)
        old_rows_dict = {{row['id']: row for row in result}}


        delete_query = f"DELETE FROM `{{modal_name}}` WHERE id IN ({{placeholders}})"
        cursor.execute(delete_query, ids)

        # Log operation
        log_entry = {{
            'user_id': user_id,
            'operation_type': 'DELETE',
            'operation_details': {{'old': [old_rows_dict.get(entry_id, {{}}) for entry_id in ids]}}
        }}
        bulk_log_operation(modal_name, [log_entry])

        conn.commit()
        response.content_type = 'application/json'
        return {{"status": "success"}}

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error deleting entries: {{e}}")
        response.status = 500
        response.content_type = 'application/json'
        return {{"status": "error", "message": str(e)}}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def log_operation(modal_name, user_id, operation_type, operation_details):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {{modal_name}}_logs (user_id, operation_type, operation_details) VALUES (%s, %s, %s)", [user_id, operation_type, json.dumps(operation_details, default=serialize_datetime)])
    conn.commit()
    cursor.close()
    conn.close()

def bulk_log_operation(modal_name, log_entries):
    if not log_entries:
        return
    values_list = [(entry['user_id'], entry['operation_type'], json.dumps(entry['operation_details'], default=serialize_datetime)) for entry in log_entries]

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    placeholders = "(%s, %s, %s)"
    insert_query = f"INSERT INTO {{modal_name}}_logs (user_id, operation_type, operation_details) VALUES {{', '.join([placeholders] * len(values_list))}}"
    flat_values = [val for sublist in values_list for val in sublist]

    cursor.execute(insert_query, flat_values)
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
        except requests.exceptions.HTTPError:
            if response.status_code == 422 and "conflicting zone" in response.json().get("errors", {}).get("name", [])[0]:
                return get_dns_zone_id(domain)
            else:
                raise
        return response.json()["id"]

    def get_dns_records(zone_id):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records"
        headers = {"Authorization": f"Bearer {netlify_key}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def delete_dns_record(zone_id, record_id):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records/{record_id}"
        headers = {"Authorization": f"Bearer {netlify_key}"}
        response = requests.delete(url, headers=headers)
        response.raise_for_status()

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
    full_domain = backend_domain  # Store full domain for precise matching

    print(f"Parent domain: {parent_domain}")
    print(f"Subdomain: {subdomain}")
    print(f"Full domain: {full_domain}")

    zone_id = get_dns_zone_id(parent_domain)
    if zone_id is None:
        zone_id = create_dns_zone(parent_domain)
        if zone_id is None:
            time.sleep(5)  # Adding a short delay before retrying
            zone_id = get_dns_zone_id(parent_domain)
        if zone_id is None:
            raise ValueError(f"Unable to retrieve DNS zone ID for {parent_domain} after creation attempt.")

    print(f"DNS Zone ID: {zone_id}")

    dns_records = get_dns_records(zone_id)
    print(f"DNS Records: {dns_records}")

    # Delete existing A records for the exact full domain match
    for record in dns_records:
        print(f"Checking record: {record}")
        if record["type"] == "A" and record["hostname"] == full_domain:
            print(f"Deleting DNS record {record['id']} for {full_domain}")
            delete_dns_record(zone_id, record["id"])

    # Verify deletion
    dns_records_after_deletion = get_dns_records(zone_id)
    for record in dns_records_after_deletion:
        if record["type"] == "A" and record["hostname"] == full_domain:
            print(f"Record still exists after deletion attempt: {record}")
        else:
            print(f"Record successfully deleted or not present: {record}")

    # Create new DNS record
    create_dns_record(zone_id, subdomain, vm_host)
    print(f"Created DNS record for {subdomain} pointing to {vm_host}")


def upload_to_gcs(ssh_key_path, gcs_instance, deploy_path):
    # Create the deployment directory on the remote server
    subprocess.run(['ssh', '-i', ssh_key_path, gcs_instance, f'mkdir -p {deploy_path}'])

    # Copy the app.py file to the remote server
    subprocess.run(['scp', '-i', ssh_key_path, 'app.py', f'{gcs_instance}:{deploy_path}/'])

    # Wait for the server to start
    time.sleep(3)


def deploy_to_gcs(vm_preset, backend_vm_deploy_path, project_name, new_db_name, backend_domain):

    location_of_app_file_on_vm = f"{backend_vm_deploy_path}/app.py"
    # app_name = project_name
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

        # Prepare test data, ensuring 'user_id' is included
        test_data = {col: "xxxxx" for col in columns_list}
        test_data['user_id'] = 1  # Ensure user_id is included
        print(f"Test data for modal {modal}: {test_data}")

        print(f"\nTesting {modal} modal:\n")

        # Create entry
        create_response = subprocess.run([
            'curl', '-X', 'POST', f"{base_url}/create/{modal}",
            '-H', "Content-Type: application/json",
            '-d', json.dumps(test_data)
        ], capture_output=True, text=True)
        print("CREATE Entry Response:", create_response.stdout)

        # Read entries
        for route in read_routes:
            route_name = list(route.keys())[0]
            read_response = subprocess.run(['curl', '-X', 'GET', f"{base_url}/read/{modal}/{route_name}"], capture_output=True, text=True)
            print(f"READ Entries Response for {route_name}:", read_response.stdout)

        # Update entry
        updated_data = {col: "xxxxx_updated" for col in columns_list}
        updated_data['user_id'] = 1  # Ensure user_id is included
        print(f"Updated data for modal {modal}: {updated_data}")
        update_response = subprocess.run([
            'curl', '-X', 'PUT', f"{base_url}/update/{modal}/1",
            '-H', "Content-Type: application/json",
            '-d', json.dumps(updated_data)
        ], capture_output=True, text=True)
        print("UPDATE Entry Response:", update_response.stdout)

        # Delete entry
        delete_response = subprocess.run([
            'curl', '-X', 'DELETE', f"{base_url}/delete/{modal}/1",
            '-H', "Content-Type: application/json",
            '-d', json.dumps({'user_id': 1})
        ], capture_output=True, text=True)
        print("DELETE Entry Response:", delete_response.stdout)


def cleanup_db(config, modal_map):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    # Delete from users where username contains 'xxxxx'
    user_query = "DELETE FROM users WHERE username LIKE '%%xxxxx%%'"
    print(f"Delete query for users: {user_query}")
    cursor.execute(user_query)

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
        # log_conditions = " OR ".join([f"operation_details LIKE '%%{col}%%xxxxx%%'" for col in columns_list])
        log_query = f"DELETE FROM {modal_name}_logs WHERE operation_details LIKE '%xxxxx%'"
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

    # modal_map["users"] = "username,password,type"

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
    # run_tests(f"https://{backend_domain}", modal_map)

    cleanup_db(db_config, modal_map)
    print(f"Backend deployed to: https://{backend_domain}")
