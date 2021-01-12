from flask import Flask, request
from uuid import uuid4
import sqlite3
import datetime
import json

app = Flask(__name__)
dbSchema = '''
CREATE TABLE IF NOT EXISTS "kharon_requests"
(
    [Id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [requestUUID] VARCHAR(40)  NOT NULL,
    [failedToExecute] INTEGER,
    [requestedFunction] VARCHAR(40) NOT NULL,
    [requestBody] NVARCHAR(6000),
    [requestFrom] VARCHAR(40),
    [requestTo] VARCHAR(40),
    [referenceObjectId] VARCHAR(40),
    [createdDatetime] TEXT,
    [headers] TEXT
);'''


def store_in_database(requestUUID, requestToLog):
    requestJSON = requestToLog.get_json()
    requestHeaders = requestToLog.headers
    conn = sqlite3.connect('/etc/kharon_db/kharon.db')
    cur = conn.cursor()
    moment = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()
    req = {
        'requestUUID': requestUUID,
        'requestBody': json.dumps(requestJSON),
        'headers': json.dumps(requestHeaders),
        'requestedFunction': requestJSON['Function'],
        'requestFrom': requestJSON['From'],
        'requestTo': requestJSON['To'],
        'createdDatetime': str(moment)
    }
    columns = ', '.join(req.keys())
    placeholders = ':' + ', :'.join(req.keys())
    query = 'INSERT INTO kharon_requests (%s) VALUES (%s)' % (columns, placeholders)
    cur.execute(query, req)
    conn.commit()
    conn.close()
    return True


@app.route('/api', methods=['POST', 'GET'])
def handle_request():
    if request.method == 'POST':
        if request.is_json:
            request_uuid = uuid4()
            success = store_in_database(str(request_uuid), request)
            if success:
                return json.dumps({'RequestCreated': str(request_uuid)}), 200
            else:
                return json.dumps({'RequestNotCreated': str(-1)}), 502
    return

