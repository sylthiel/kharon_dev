import configparser
from simple_salesforce import Salesforce, SalesforceResourceNotFound
import json


class RequestHandlerBase:
    def __init__(self, resource_name, request_body, request_uuid):
        self.resource_name = resource_name
        self.is_json = None
        try:
            self.request = json.loads(request_body)
            self.is_json = True
        except TypeError:
            self.request = None
            self.is_json = False

        self.requestId = request_uuid
        self.connection_object = None
        config = configparser.ConfigParser()
        config.read('kh.ini')
        self.config = {}
        try:
            for x in config[resource_name]:
                self.config[x] = config[resource_name][x]
        except KeyError as e:
            with open('debug.log', 'w+') as debug:
                debug.write(f'{request_uuid}|'
                            f'  Failed to extract configuration for resource {resource_name} -- invalid resource name')
            raise
        with open('debug.log', 'w+') as debug:
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
            existing_case = self.connection_object.YoutrackIssue__c.get_by_custom_id('YTReadableId__c', yti_details['YTReadableId'])
            return self.connection_object.YoutrackIssue__c.update(existing_case['Id'], prepared_yti_details)
        except SalesforceResourceNotFound:
            return self.connection_object.YoutrackIssue__c.create(prepared_yti_details)

