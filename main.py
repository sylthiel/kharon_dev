import sqlite3
import configparser
from request_handler_base import SalesforceRequestHandler, YoutrackRequestHandler, SlackRequestHandler, ProductBoardRequestHandler
import datetime
import json
import time

handler_association = {
    'Salesforce': SalesforceRequestHandler,
    'YouTrack': YoutrackRequestHandler,
    'Slack': SlackRequestHandler,
    'ProductBoard': ProductBoardRequestHandler
}
global_request_handlers = {

}
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


def dbg(debug_output, log_type='debug'):
    with open(log_type+'.txt', 'a+') as debug:
        debug.write(debug_output)


def validate_request(request_uuid, request_body):
    try:
        json_req = json.loads(request_body)
    except Exception as e:
        dbg(f'{request_uuid}|INFO|Error when parsing request JSON\n{str(e)}\nRequest body:\n'
            f'{request_body}\n', 'request_validation')
        return False
    sender, rcpt, fn = json_req.get('From'), json_req.get('To'), json_req.get('Function')
    return sender and rcpt and fn


def load_config():
    global db_cfg
    config = configparser.ConfigParser()
    config.read('kh.ini')
    db_cfg = {key: config['Database information'][key] for key in config['Database information']}


def process(kh_request):
    global global_request_handlers
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
    if request_body['To'] not in global_request_handlers:
        global_request_handlers[request_body['To']] = \
            handler_association[request_body['To']](request_body, request_uuid)
    else:
        global_request_handlers[request_body['To']].update_request(request_body, request_uuid)
    dbg(f'{request_uuid}|INFO|Starting processing for request \n')
    result = global_request_handlers[request_body['To']].function_association[request_body['Function']]()

    # This condition indicates a request that has more than one stage (needs to be processed further)
    if validate_request(request_uuid, result):
        return process([request_uuid, result, failed_to_execute])

    if result:
        dbg(f'{request_uuid}|INFO|Successfully completed\n')
        cur.execute('UPDATE kharon_requests SET Completed = 1 WHERE requestUUID = ?',
                    [request_uuid])
    else:
        dbg(f'{request_uuid}|ERROR|Failed to complete, currently at {failed_to_execute+1} retries')
        cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUID = ?',
                    (failed_to_execute+1, request_uuid))
    con.commit()
    con.close()
    return True


def processing_loop():
    load_config()
    global global_request_handlers
    while True:
        con = sqlite3.connect(db_cfg['database_path'])
        if con:
            cur = con.cursor()
            query = 'SELECT requestUUID, requestBody, failedToExecute FROM kharon_requests ' \
                    'WHERE Completed = 0 AND failedToExecute < 3 ORDER BY createdDatetime LIMIT 100'
            cur.execute(query)
            current_requests = cur.fetchall()

            if len(current_requests):
                # rqh = handler_association[request_body['To']](request_body, request_uuid)

                for it_request in current_requests:
                    request_uuid, request_body = it_request[0], json.loads(it_request[1])
                    # if request_body['To'] not in global_request_handlers:
                    #     global_request_handlers[request_body['To']] = handler_association[request_body['To']](
                    #         request_body, request_uuid)
                    # else:
                    #     global_request_handlers[request_body['To']].update_request(request_body, request_uuid)
                    try:
                        process(it_request)
                    except Exception as e:
                        dbg(f'{it_request[0]}|ERROR|'
                            f'Exception while processing request\n{getattr(e,"message", repr(e))}\n')
                        cur.execute('UPDATE kharon_requests SET failedToExecute = ? WHERE requestUUID = ?',
                                    (int(it_request[2]) + 1, it_request[0]))
                        con.commit()
                global_request_handlers = {}
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
