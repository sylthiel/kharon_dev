import sqlite3
import configparser
from request_handler_base import SalesforceRequestHandler
import datetime

handler_association = {'Salesforce': SalesforceRequestHandler}
db_cfg = {}


def load_config():
    global db_cfg
    config = configparser.ConfigParser()
    config.read('kh.ini')
    db_cfg = {key: config['Database information'][key] for key in config['Database information']}


def process(kh_request):
    request_uuid, request_body, failed_to_execute = kh_request[0], kh_request[1], kh_request[2]
    rqh = handler_association[request_body['To']](request_body, request_uuid)
    result = rqh.function_association[request_body['Function']]()

    con = sqlite3.connect(db_cfg['database_path'])
    cur = con.cursor()
    if result:
        cur.execute('UPDATE ? SET Completed = 1 WHERE request_uuid = ?',
                    db_cfg['table_name'], request_uuid)
    else:
        cur.execute('UPDATE ? SET failedToExecute WHERE request_uuid = ?',
                    db_cfg['table_name'], failed_to_execute+1)
    con.commit()
    con.close()
    return True


def processing_loop():
    load_config()
    while True:
        try:
            con = sqlite3.connect(db_cfg['database_path'])
            if con:
                cur = con.cursor()
                cur.execute('SELECT requestUUID, requestBody, failedToExecute FROM ? '
                            'WHERE Completed = 0 AND failedToExecute < 3 LIMIT 10 ORDER BY createdDatetime',
                            db_cfg['table_name'])
                current_requests = cur.fetchall()
                if len(current_requests):
                    for it_request in current_requests:
                        process(it_request)
            else:
                with open('debug.txt', 'w+') as debug:
                    debug.write(f'{datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()}'
                                f'|CRITICAL|Failed to establish a database connection\n')

        except Exception as e:
            with open('debug.txt', 'w+') as debug:
                debug.write('Failed to process request\n' + str(e) + '\n')
            continue