import configparser
import requests
from simple_salesforce import Salesforce, SalesforceResourceNotFound
import json


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
            if self.config['sandbox']:
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
        prepared_yti_details = {k + '__c': v for k, v in yti_details.items()}
        prepared_yti_details['Name'] = yti_details['YTReadableId']

        try:
            existing_case = self.connection_object.YoutrackIssue__c.get_by_custom_id('YTReadableId__c',
                                                                                     yti_details['YTReadableId'])
            return self.connection_object.YoutrackIssue__c.update(existing_case['Id'], prepared_yti_details)
        except SalesforceResourceNotFound:
            return self.connection_object.YoutrackIssue__c.create(prepared_yti_details)


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
            'mention_case_in_yti': self.mention_case_in_yti
        }

    def obtain_yti_details(self):
        issue_api_location = self.api_endpoint + '/issues/' + self.request['YTReadableId']
        issue_with_fields = issue_api_location + '?fields=id,summary,' \
                                                 'customFields(id,' \
                                                 'projectCustomField(id,field(id,name)),value(name))'
        request_yti_details = requests.get(issue_with_fields, headers=self.headers)
        if request_yti_details.status_code != 200:
            if request_yti_details.status_code == 404:
                yti_details = {
                    'YTReadableId': self.request['YTReadableId'],
                    'State': 'Non-existent'
                }
                return yti_details
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
                        yti_details[name] = value
                except IndexError:
                    with open('debug.txt', 'a+') as log:
                        log.write(f'Unexpected json structure:\n'
                                  f'{str(cf)}')
            yti_main = {
                "From": "YouTrack",
                "To": self.request.get('From'),
                "Function": "populate_yti_details",
                "YoutrackIssue": yti_details}
            return json.dumps(yti_main)

    def mention_case_in_yti(self):
        issue_comments_api_location = self.api_endpoint + '/issues/' + self.request['YTReadableId'] + '/comments'
        case_information = self.request.get('CaseInformation')
        customer_information = case_information.get('CustomerInformation')

        comment_text = {
            "text": f'This issue has been referenced in [Salesforce]({case_information.get("URL")})\n'
                    f'Affected customer: {customer_information.get("CompanyName")}\n'
                    f'({customer_information.get("ContactEmail")})\n'
                    f'Annual: ${customer_information.get("Annual$")}\n'
                    f'Engineer comment: {case_information.get("CommentFromEngineer")}\n'
                    f'Case Assignee: {case_information.get("Assignee")}\n\n'
                    f'This comment was generated automatically by kh'
        }
        json_comment = json.dumps(comment_text)
        post_comment_request = requests.post(issue_comments_api_location, data=json_comment, headers=self.headers)

        return str(post_comment_request.status_code)