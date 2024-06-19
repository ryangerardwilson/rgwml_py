
import pymysql
import json
from bottle import Bottle, request, response, run, error, HTTPResponse
from datetime import datetime

config = {'host': '34.131.124.18', 'user': 'wiomsudo', 'password': 'wiomsudo', 'database': 'labsforge'}

modal_map = {'customers': 'mobile,issue,status', 'partners': 'mobile,issue,status'}

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
        cursor.execute("SELECT id, username, password FROM users WHERE username = %s", (username,))
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

@app.route('/read/<modal_name>', method=['OPTIONS', 'GET'])
def read_entries(modal_name):
    if request.method == 'OPTIONS':
        return {}

    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    if modal_name != 'favicon':
        print(f"SELECT * FROM {modal_name}")
        cursor.execute(f"SELECT * FROM {modal_name}")
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

run(app, host='0.0.0.0', port=8080)
