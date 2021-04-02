import sqlite3
import configparser
from request_handler_base import SalesforceRequestHandler, YoutrackRequestHandler
import datetime
import json
import time

handler_association = {'Salesforce': SalesforceRequestHandler, 'YouTrack': YoutrackRequestHandler}
db_cfg = {}


db_schema = '''
CREATE TABLE IF NOT EXISTS "kharon_requests"
(
    [Id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    [requestUUID] VARCHAR(40)  NOT NULL,
    [failedToExecute] INTEGER DEFAULT 0,
    [requestedFunction] VARCHAR(40) NOT NULL,
    [requestBody] NVARCHAR(6000),
    [requestFrom] VARCHAR(40),
    [requestTo] VARCHAR(40),
    [referenceObjectId] VARCHAR(40),
    [createdDatetime] TEXT,
    [headers] TEXT,
    [Completed] INTEGER DEFAULT 0);
'''


def dbg(debug_output):
    with open('debug.txt', 'a+') as debug:
        debug.write(debug_output)


def validate_request(request_uuid, request_body):
    try:
        json_req = json.loads(request_body)
    except Exception as e:
        dbg(f'{request_uuid}|INFO|Error when parsing request JSON\n{str(e)}\nRequest body:\n'
            f'{request_body}\n')
        return False
    sender, rcpt, fn = json_req.get('From'), json_req.get('To'), json_req.get('Function')
    return sender and rcpt and fn


def load_config():
    global db_cfg
    config = configparser.ConfigParser()
    config.read('kh.ini')
    db_cfg = {key: config['Database information'][key] for key in config['Database information']}


def process(kh_request):
    con = sqlite3.connect(db_cfg['database_path'])
    cur = con.cursor()
    if not validate_request(kh_request[0], kh_request[1]):
        cur.execute('UPDATE kharon_requests SET failedToExecute = 3 WHERE requestUUID = ?',
                    [kh_request[0]])
        dbg(f'{kh_request[0]}|ERROR|Request was invalid, discarding')
        con.commit()
        con.close()
        return False
    request_uuid, request_body, failed_to_execute = kh_request[0], json.loads(kh_request[1]), kh_request[2]
    if request_body['To'] != 'db':
        rqh = handler_association[request_body['To']](request_body, request_uuid)
        dbg(f'{request_uuid}|INFO|Starting processing for request \n')
        result = rqh.function_association[request_body['Function']]()

        # This condition indicates a request that has more than one stage (needs to be processed further)
        if validate_request(request_uuid, result):
            return process([request_uuid, result, failed_to_execute])

        if result:
            dbg(f'{request_uuid}|INFO|Successfully completed\n')
            cur.execute('UPDATE kharon_requests SET Completed = 1 WHERE requestUUID = ?',
                        [request_uuid])
        else:
            dbg(f'{request_uuid}|ERROR|Failed to complete, currently at {failed_to_execute+1} retries')
            cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUIDit = ?',
                        (failed_to_execute+1, request_uuid))
        con.commit()
        con.close()
        return True
    else:
        req_body = {x: request_body[x] for x in request_body if x not in {'From', 'To', 'Function'}}
        columns = ', '.join(req_body.keys())
        placeholders = ':' + ', :'.join(req_body.keys())
        query = 'INSERT INTO yt_comments (%s) VALUES (%s)' % (columns, placeholders)
        dbg(f'{request_uuid}|Constructed query: {query}')
        cur.execute(query, req_body)
        cur.execute('UPDATE kharon_requests SET Completed = 1 WHERE requestUUID = ?',
                    [request_uuid])
        dbg(f'{request_uuid}|Youtrack comment {req_body["created_comment_id"]} logged to database')
        con.commit()
        con.close()
        return True



def processing_loop():
    load_config()
    while True:
        con = sqlite3.connect(db_cfg['database_path'])
        if con:
            cur = con.cursor()
            query = 'SELECT requestUUID, requestBody, failedToExecute FROM kharon_requests ' \
                    'WHERE Completed = 0 AND failedToExecute < 3 ORDER BY createdDatetime LIMIT 10'
            cur.execute(query)
            current_requests = cur.fetchall()

            if len(current_requests):
                for it_request in current_requests:
                    try:
                        process(it_request)
                    except Exception as e:
                        dbg(f'{it_request[0]}|ERROR|'
                            f'Exception while processing request\n{getattr(e,"message", repr(e))}\n')
                        cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUID = ?',
                                    (int(it_request[2]) + 1, it_request[0]))
                        con.commit()
            else:
                time.sleep(1)
        else:
            dbg(f'{datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()}'
                f'|CRITICAL|Failed to establish a database connection\n')
            load_config()
            dbg(f'{datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()}'
                f'|INFO|Config reloaded due to connection error\nCurrent config:\n{db_cfg}')

if __name__ == "__main__":
    processing_loop()
