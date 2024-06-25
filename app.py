
import pymysql
import json
from bottle import Bottle, request, response, run, error, HTTPResponse
from datetime import datetime

config = {'host': '34.131.124.18', 'user': 'wiomsudo', 'password': 'wiomsudo', 'database': 'sajal_ka_crm'}

modal_map = {'social_media_escalations': {'columns': 'url[VARCHAR(2048)],forum,mobile,issue,status,sub_status,action_taken,follow_up_date', 'read_routes': [{'most-recent-500': "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations ORDER BY id DESC LIMIT 500"}, {'todays-cases': "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations WHERE DATE(CONVERT_TZ(created_at, '+00:00', '+05:30')) = CURDATE() ORDER BY id ASC"}, {'yesterdays-cases': "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations WHERE DATE(CONVERT_TZ(created_at, '+00:00', '+05:30')) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY id ASC"}]}, 'users': {'columns': 'username,password,type', 'read_routes': [{'default': 'SELECT * FROM users'}]}}

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
    return {"status":"success"}

@app.route('/authenticate', method=['POST', 'OPTIONS'])
def authenticate():
    if request.method == 'OPTIONS':
        return {}

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
            return {"status": "success", "message": "Authentication successful", "user": user}
        else:
            return {"status": "error", "message": "Invalid username or password"}
    except pymysql.MySQLError as e:
        return {"status": "error", "message": str(e)}

@app.route('/create/<modal_name>', method=['OPTIONS', 'POST'])
def create_entry(modal_name):
    if request.method == 'OPTIONS':
        return {}

    data = request.json
    columns = modal_map[modal_name].split(",")
    column_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    values = [data.get(col) for col in columns]

    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {modal_name} (user_id, {column_names}) VALUES (%s, {placeholders})", [data['user_id']] + values)
    conn.commit()
    cursor.close()
    conn.close()

    log_operation(modal_name, data['user_id'], 'CREATE', {"user_id": data['user_id'], **data})
    return {"status": "success"}

@app.route('/read/<modal_name>/<route_name>', method=['OPTIONS', 'GET'])
def read_entries(modal_name, route_name):
    if request.method == 'OPTIONS':
        return {}

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
                raise ValueError(f"Read route '{route_name}' not found for modal '{modal_name}'")
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

            return {"columns": column_names, "data": serialized_result}
    except Exception as e:
        response.status = 500
        return {"error": str(e)}



@app.route('/query/<modal_name>', method=['OPTIONS', 'POST'])
def query_entries(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Allow'] = 'OPTIONS, POST'
        return {}

    data = request.json
    if not data or 'query_string' not in data:
        response.status = 400
        return {"error": "query_string is required in the request body"}

    query_string = data['query_string'].strip()
    lower_query_string = query_string.lower()
    expected_start = f"select * from {modal_name}".lower()

    if not lower_query_string.startswith(expected_start):
        response.status = 400
        return {"error": f"query_string must start with 'SELECT * FROM {modal_name}'"}

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

        return {"columns": column_names, "data": serialized_result}
    except Exception as e:
        response.status = 500
        return {"error": str(e)}



@app.route('/search/<modal_name>', method=['OPTIONS', 'POST'])
def search_entries(modal_name):
    if request.method == 'OPTIONS':
        response.headers['Allow'] = 'OPTIONS, POST'
        return {}

    if request.method == 'POST':
        data = request.json
        if not data or 'search_string' not in data:
            response.status = 400
            return {"error": "search_string is required in the request body"}

        search_string = data['search_string']
        try:
            conn = pymysql.connect(**config)
            cursor = conn.cursor()

            # Retrieve column names of the table
            cursor.execute(f"SHOW COLUMNS FROM {modal_name}")
            columns = [row[0] for row in cursor.fetchall()]

            # Construct the WHERE clause to search across all columns
            where_clause = " OR ".join([f"{col} LIKE '%{search_string}%'" for col in columns])
            query = f"SELECT * FROM {modal_name} WHERE {where_clause}"

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

            return {"columns": column_names, "data": serialized_result}
        except Exception as e:
            response.status = 500
            return {"error": str(e)}

@app.route('/update/<modal_name>/<entry_id>', method=['PUT','OPTIONS'])
def update_entry(modal_name, entry_id):

    if request.method == 'OPTIONS':
        return {}

    data = request.json
    columns = modal_map[modal_name].split(",")
    
    # Remove columns that should not be updated
    columns = [col for col in columns if col not in ['id', 'created_at', 'updated_at', 'user_id']]

    # Construct the SET clause with placeholders for SQL parameters
    set_clause = ", ".join([f"{col} = %s" for col in columns])
    values = [data.get(col) for col in columns]

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    try:
        # Fetch the old row for logging purposes
        cursor.execute(f"SELECT * FROM {modal_name} WHERE id = %s", [entry_id])
        old_row = cursor.fetchone()
        print(old_row)

        # Perform the update operation
        cursor.execute(f"UPDATE {modal_name} SET {set_clause} WHERE id = %s", values + [entry_id])
        conn.commit()

        # Log the operation
        log_operation(modal_name, data['user_id'], 'UPDATE', {"old": old_row, "new": data})

        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

@app.route('/delete/<modal_name>/<entry_id>', method=['OPTIONS', 'DELETE'])
def delete_entry(modal_name, entry_id):

    if request.method == 'OPTIONS':
        return {}

    data = request.json
    user_id = data.get('user_id')
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {modal_name} WHERE id = %s", [entry_id])
    old_row = cursor.fetchone()
    cursor.execute(f"DELETE FROM {modal_name} WHERE id = %s", [entry_id])
    conn.commit()
    cursor.close()
    log_operation(modal_name, user_id, 'DELETE', { "deleted_row": old_row })
    return {"status": "success"}

def log_operation(modal_name, user_id, operation_type, operation_details):
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {modal_name}_logs (user_id, operation_type, operation_details) VALUES (%s, %s, %s)", [user_id, operation_type, json.dumps(operation_details, default=serialize_datetime)])
    conn.commit()
    cursor.close()
    conn.close()

