import configparser
import requests
from simple_salesforce import Salesforce, SalesforceResourceNotFound
import json
import datetime
import io
import zipfile
import sqlite3

def dbg(debug_output):
    with open('debug.txt', 'a+') as debug:
        debug.write(debug_output)

class RequestHandlerBase:
    def __init__(self, resource_name, request_body, request_uuid):
        self.resource_name = resource_name
        self.is_json = None
        self.request = request_body
        self.requestId = request_uuid
        self.connection_object = None
        config = configparser.ConfigParser()
        config.read('kh.ini')
        self.config = {}
        try:
            for x in config[resource_name]:
                self.config[x] = config[resource_name][x]
        except KeyError as e:
            with open('debug.log', 'a+') as debug:
                debug.write(f'{request_uuid}|'
                            f'  Failed to extract configuration for resource {resource_name} -- invalid resource name')
            raise
        with open('debug.log', 'a+') as debug:
            debug.write(f'{request_uuid}|'
                        f'SUCCESS: loaded request handler configuration for {resource_name}')


class SalesforceRequestHandler(RequestHandlerBase):

    def __init__(self, request, request_uuid):
        super().__init__('Salesforce', request, request_uuid)
        self.function_association = {'populate_yti_details': self.populate_yti_details}
        self.connect()

    def connect(self):
        if self.config != {}:
            if 'sandbox' in self.config:
                self.connection_object = Salesforce(username=self.config['username'],
                                                    password=self.config['password'],
                                                    security_token=self.config['security_token'],
                                                    domain='test')
            else:
                self.connection_object = Salesforce(username=self.config['username'],
                                                    password=self.config['password'],
                                                    security_token=self.config['security_token'])

    def populate_yti_details(self):
        """This method takes a property map like and updates the associated YoutrackIssue__c object accordingly
           Format:
           {"YoutrackIssue":{"property_name":"property_value"}}"""
        yti_details = self.request.get('YoutrackIssue')
        if 'old_issue_id' not in yti_details:
            prepared_yti_details = {k + '__c': v for k, v in yti_details.items()}
            correct_id = yti_details['YTReadableId']
        else:
            prepared_yti_details = {k + '__c': v for k, v in yti_details.items() if k != 'old_issue_id'}
            correct_id = yti_details['old_issue_id']
        prepared_yti_details['Name'] = yti_details['YTReadableId']
        if 'project__c' not in prepared_yti_details:
            prepared_yti_details['project__c'] = yti_details['YTReadableId'].split('-')[0]
        try:
            existing_case = self.connection_object.YoutrackIssue__c.get_by_custom_id('YTReadableId__c',
                                                                                     correct_id)
            return self.connection_object.YoutrackIssue__c.update(existing_case['Id'], prepared_yti_details)
        except SalesforceResourceNotFound:
            return self.connection_object.YoutrackIssue__c.create(prepared_yti_details)


class KharonDatabaseHandler:
    def __init__(self):
        self.db_cfg = KharonDatabaseHandler.load_config()

    @staticmethod
    def load_config():
        config = configparser.ConfigParser()
        config.read('kh.ini')
        return {key: config['Database information'][key] for key in config['Database information']}

    def log_yt_comment(self, request_uuid, request_body):
        con = sqlite3.connect(self.db_cfg['database_path'])
        cur = con.cursor()
        req_body = {x: request_body[x] for x in request_body if x not in {'From', 'To', 'Function'}}
        columns = ', '.join(req_body.keys())
        placeholders = ':' + ', :'.join(req_body.keys())
        query = 'INSERT INTO yt_comments (%s) VALUES (%s)' % (columns, placeholders)
        # dbg(f'{request_uuid}|Constructed query: {query}')
        cur.execute(query, req_body)
        dbg(f'{request_uuid}|Youtrack comment {req_body["created_comment_id"]} logged to database')
        con.commit()
        con.close()

    def mark_comment_as_deleted(self, comment_number):
        con = sqlite3.connect(self.db_cfg['database_path'])
        cur = con.cursor()
        cur.execute(f'UPDATE yt_comments SET status = 2 WHERE number = ?', (comment_number,))
        cur.commit()
        con.close()

    def find_latest_comment(self, trigger_object_id, trigger_yt_id):
        db_s = '''
                CREATE TABLE yt_comments(
                    [trigger_object] NVARCHAR(40) NOT NULL,
                    [request_uuid] VARCHAR(40) NOT NULL,
                    [engineer_comment] NVARCHAR(6000) NOT NULL,
                    [created_datetime] TEXT,
                    [created_comment_id] VARCHAR(40),
                    [number] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    [created_comment_path] NVARCHAR(60) NOT NULL 
                );
                '''
        con = sqlite3.connect(self.db_cfg['database_path'])
        cur = con.cursor()
        cur.execute(f'SELECT created_comment_path, number FROM yt_comments WHERE trigger_object = ? AND status = 1 '
                    f'ORDER BY created_datetime DESC LIMIT 1', (trigger_object_id,))
        located_comments = cur.fetchall()
        con.close()
        return located_comments


class YoutrackRequestHandler(RequestHandlerBase):
    def __init__(self, request_body, request_uuid):
        super().__init__('YouTrack', request_body, request_uuid)
        self.headers = {
            'Accept': 'application/json',
            'Authorization': self.config['authorization'],
            'Cache-control': 'no-cache',
            'Content-type': 'application/json'
        }
        self.required_details = {it.strip() for it in self.config['required details'].split(',')}
        self.api_endpoint = self.config['api endpoint']
        self.function_association = {
            'obtain_yti_details': self.obtain_yti_details,
            'mention_case_in_yti': self.mention_case_in_yti,
            'delete_kh_yt_comment': self.delete_kh_yt_comment
        }

    def obtain_yti_details(self):
        issue_api_location = self.api_endpoint + '/issues/' + self.request['YTReadableId']
        issue_with_fields = issue_api_location + '?fields=id,summary,' \
                                                 'customFields(id,' \
                                                 'projectCustomField(id,field(id,name)),value(name)),tags(id,name)'
        request_yti_details = requests.get(issue_with_fields, headers=self.headers)
        print(request_yti_details.json())
        if request_yti_details.status_code != 200:
            if request_yti_details.status_code == 404:
                yti_details = {
                    'YTReadableId': self.request['YTReadableId'],
                    'State': 'Non-existent'
                }
                yti_main = {
                    'From': 'YouTrack',
                    'To': self.request.get('From'),
                    'Function': 'populate_yti_details',
                    'YoutrackIssue': yti_details
                }
                return json.dumps(yti_main)
            else:
                with open('debug.txt', 'a+') as log:
                    log.write(f'{self.requestId}|ERROR|'
                              f'Failed to obtain details for issue {self.request["YTReadableId"]}\n'
                              f'API URL used: {issue_with_fields}\n'
                              f'Status code: {request_yti_details.status_code}'
                              f'Server response: {request_yti_details.text}')
                return None

        else:
            yti_details = {
                'YTReadableId': self.request['YTReadableId']
            }
            json_response = request_yti_details.json()
            if 'summary' in json_response:
                yti_details['summary'] = json_response['summary']

            custom_fields = json_response['customFields']
            for cf in custom_fields:
                try:
                    name = cf['projectCustomField']['field']['name']
                    if name in self.required_details and cf['value']:
                        value = cf['value']['name']
                        yti_details[name.replace(' ', '_')] = value
                except IndexError:
                    with open('debug.txt', 'a+') as log:
                        log.write(f'Unexpected json structure:\n'
                                  f'{str(cf)}')
            for tag in json_response['tags']:
                if tag['name'] in self.required_details:
                    yti_details[tag['name'].replace(' ', '_')] = True
            yti_main = {
                "From": "YouTrack",
                "To": self.request.get('From'),
                "Function": "populate_yti_details",
                "YoutrackIssue": yti_details}
            return json.dumps(yti_main)

    def mention_case_in_yti(self):
        """Creates an automated comment in the YT Issue referenced in the JSON request provided and logs
        it in yt_comments db
        Request format:
        {
            'TriggerObject': 'SF Object ID',
            'YTReadableId': 'SF-200',
            'CaseInformation': {
                'URL': 'http://case_url.com',
                'CommentFromEngineer': 'Sample comment text',
                'CustomerInformation':{
                    'Annual$': 14069,
                    'CompanyName': 'Some Company Name',
                    'ContactEmail': 'khdev@khdev.msp360'
                }
            }
        }
        """
        issue_comments_api_location = self.api_endpoint + '/issues/' + self.request['YTReadableId'] + '/comments'
        case_information = self.request.get('CaseInformation')
        customer_information = case_information.get('CustomerInformation')
        comment_text = {"text" : f'This issue has been referenced in [Salesforce]({case_information.get("URL")})\n'
                                 f'Affected customer: {customer_information.get("CompanyName")}\n'
                                 f'({customer_information.get("ContactEmail")})\n'}
        if customer_information.get("Annual$") != '':
            comment_text['text'] += f'Annual: ${customer_information.get("Annual$")}\n'
        if case_information.get("CommentFromEngineer") is not None:
            if hasattr(case_information.get("CommentFromEngineer"), 'isspace'):
                if not case_information.get("CommentFromEngineer").isspace():
                    comment_text['text'] += f'Engineer comment: {case_information.get("CommentFromEngineer")}\n'
        comment_text['text'] += 'This comment was generated automatically by kh'
        json_comment = json.dumps(comment_text)
        post_comment_request = requests.post(issue_comments_api_location, data=json_comment, headers=self.headers)
        # print(post_comment_request.text)
        response = post_comment_request.json()
        db_s = '''
        CREATE TABLE yt_comments(
            [trigger_object] NVARCHAR(40) NOT NULL,
            [request_uuid] VARCHAR(40) NOT NULL,
            [engineer_comment] NVARCHAR(6000) NOT NULL,
            [created_datetime] TEXT,
            [created_comment_id] VARCHAR(40),
            [number] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            [created_comment_path] NVARCHAR(60) NOT NULL,
            [trigger_yt_id] NVARCHAR(50),
            [status] INTEGER DEFAULT 0
        );
        '''
        comment_for_db = {
            'trigger_object': self.request['TriggerObject'],
            'request_uuid': self.requestId,
            'engineer_comment': comment_text['text'],
            'created_datetime': str(datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()),
            'created_comment_id': response['id'],
            'created_comment_path': f"{issue_comments_api_location}/{response['id']}",
            'trigger_yt_id': self.request['YTReadableId'],
            'status': 1,
            'From': 'YoutrackRequestHandler',
            'To': 'db',
            'Function': 'log_yti_comment'
        }
        kh_db = KharonDatabaseHandler()
        kh_db.log_yt_comment(self.requestId, comment_for_db)
        return True

    def delete_kh_yt_comment(self):
        """This function takes a TriggerObject Id and attempts to locate the most recent comment created by kh
        for this TriggerObject
        Request format:
        {
            'TriggerObject': 'Trigger Object ID',
            'YTReadableId': 'SF-200',
            'From': 'Salesforce',
            'To': 'YouTrack',
            'Function': delete_kh_yt_comment
        }
        """
        # issue_comments_api_location = self.api_endpoint + '/issues/' + self.request['YTReadableId'] + '/comments'
        kh_db = KharonDatabaseHandler()
        relevant_comment = kh_db.find_latest_comment(self.request['TriggerObject'], self.request['YTReadableId'])

        if relevant_comment:
            print(relevant_comment)
            delete_request = requests.post(
                relevant_comment[0][0], data=json.dumps({'deleted': True}), headers=self.headers)
            if delete_request.status_code != 200:
                dbg(f"{self.requestId}|ERROR|Failed to delete comment {relevant_comment[0][1]}"
                    f" ({relevant_comment[0][0]})")
                dbg(f"{self.requestId}|INFO|YT Response: {delete_request.text}")
                return False
            dbg(f"{self.requestId}|"
                f"INFO|Successfully deleted comment {relevant_comment[0][1]}({relevant_comment[0][0]})")
            kh_db.mark_comment_as_deleted(relevant_comment[1])
            return True
        else:
            dbg(f"{self.requestId}|INFO|No comment found for trigger_object {self.request['TriggerObject']} "
                f"and Youtrack Issue {self.request['YTReadableId']}")
        return False
