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


def load_config():
    global db_cfg
    config = configparser.ConfigParser()
    config.read('kh.ini')
    db_cfg = {key: config['Database information'][key] for key in config['Database information']}


def process(kh_request):
    request_uuid, request_body, failed_to_execute = kh_request[0], json.loads(kh_request[1]), kh_request[2]

    rqh = handler_association[request_body['To']](request_body, request_uuid)
    print(f'Starting processing for request {request_uuid}')
    result = rqh.function_association[request_body['Function']]()
    if hasattr(result, 'To'):
        return process([request_uuid, result, failed_to_execute])
    con = sqlite3.connect(db_cfg['database_path'])
    cur = con.cursor()
    if result:
        cur.execute('UPDATE kharon_requests SET Completed = 1 WHERE requestUUID = ?',
                    [request_uuid])
    else:
        cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUIDit = ?',
                    (failed_to_execute+1, request_uuid))
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
                        with open('debug.txt', 'a+') as debug:
                            debug.write(f'{it_request[0]}|ERROR|Exception while processing request\n{str(e)}\n')
                        cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUID = ?',
                                    (int(it_request[2]) + 1, it_request[0]))
                        con.commit()
            else:
                time.sleep(15)
        else:
            with open('debug.txt', 'a+') as debug:
                debug.write(f'{datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()}'
                            f'|CRITICAL|Failed to establish a database connection\n')
            load_config()
            with open('debug.txt', 'a+') as debug:
                debug.write(f'{datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()}'
                            f'|INFO|Config reloaded due to connection error\nCurrent config:\n{db_cfg}')
