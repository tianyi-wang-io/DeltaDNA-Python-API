import json
import requests
import pandas as pd


class deltaDNA:
    def __init__(self, apikey, password):
        self._apiKey = apikey
        self._password = password
        self.paramField = ['name', 'id', 'type', 'format', 'required']
        self.metricsField = ['calculatingMetricFirst',
                             'calculatingMetricLast',
                             'calculatingMetricCount',
                             'calculatingMetricMin',
                             'calculatingMetricMax',
                             'calculatingMetricSum']
        
        self._token = self._get_token()
        self._header = {'Authorization': f'Bearer {self._token}'}
        self._game_list = self.game_list()
    
    
    def _get_token(self):
        """
        Desc
            Trying to get token using api key and password.
        """
        url = 'https://api.deltadna.net/api/authentication/v1/authenticate'
        ddna = requests.post(url, json={'key': self._apiKey,
                                        'password': self._password})
        if ddna.status_code == 200:
            token = json.loads(ddna.text)['idToken']
            print('Successfully connected Deltadna.')
            return token
        else:
            print('Connection failed, error code: ', ddna.status_code)
            return None
    
    
    def game_list(self):
        """
        About Application and Environment IDs
            The Dev environment ID is always the application ID +1, 
            and the Live environment ID is always the application ID +2.
        """
        url = 'https://api.deltadna.net/api/engage/v1/environments/'
        ddna = requests.get(url, headers=self._header)
        
        if ddna.status_code != 200:
            print('Failed to connect, error code: ', ddna.status_code)
            return None
        
        games = pd.DataFrame(json.loads(ddna.text))
        games.columns = ['Game Name', 'Environment ID', 'Environment Name']
        env_infer = games['Environment Name'].replace({'Dev': 1, 'Live': 2}).values
        app_infer = games['Environment ID'].values - env_infer
        games['Application ID'] = app_infer
        
        return games
    
    
    def _event_spec(self, 
                    env_id:int):
        """
        Desc 
            return all all events information for the environmentID.
        """
        url = 'https://api.deltadna.net/api/events/v1/events'
        ddna = requests.get(url, headers=self._header)
        
        if ddna.status_code != 200:
            print('Failed to connect, error code: ', ddna.status_code)
            return None
        elif env_id not in self._game_list['Environment ID'].tolist():
            print('Environment ID not found in current API key.')
            return None
        else:
            print('Game connected :', 
                  self._game_list[self._game_list['Environment ID']==env_id]['Game Name'][0], 
                  self._game_list[self._game_list['Environment ID']==env_id]['Environment Name'][0])
        
        events = json.loads(ddna.text)
        spec = [events[i] for i in range(len(events)) if events[i]['environment']==env_id]
        
        return spec
    
    
    def event_list(self, 
                   env_id:int):
        """
        Desc
            return a list of tuples of eventName and eventID
        """
        events = self._event_spec(env_id=env_id)
        if events is None:
            return None
        
        events = [(i['name'], i['id']) for i in events]
            
        return events
    
    
    def event_details(self, 
                      env_id:int):
        """
        Desc
            return all events and parameters with details
        """
        events = self._event_spec(env_id=env_id)
        if events is None:
            return None
        
        result = []
        for event in events:
            name = event['name']
            params = event['parameters']
            detail = [[i.get(j) for j in self.paramField + self.metricsField] for i in params]
            details = [[name] + i for i in detail]
            details = pd.DataFrame(details, columns = ['eventName']+self.paramField+self.metricsField)
            result.append(details)
        result = pd.concat(result, axis=0).reset_index(drop=True)
        result.insert(0, 'Environment ID', env_id)
        result.rename(columns = {'name': 'Event Name', 'id': 'parameterID'}, inplace=True)
        
        return result
    
    
    def _get_applicationID(self, 
                           env_id:int):
        """
        Desc:
            Getting applicationID based on environmentID
        """
        return self._game_list[self._game_list['Environment ID']==env_id]['Application ID'][0]
    
    
    def _parameter_spec(self, 
                        env_id:int, 
                        params:dict):
        """
        Desc
            return all parameters for the application
        """
        url = 'https://api.deltadna.net/api/events/v1/event-parameters'
        ddna = requests.get(url, headers=self._header, params=params)
        
        if ddna.status_code != 200:
            print('Failed to connect, error code: ', ddna.status_code)
            return None
        
        if env_id not in self._game_list['Environment ID'].tolist():
            print('Environment ID not found in current API key.')
            return None
        
        parameters = json.loads(ddna.text)
        
        applicationID = self._get_applicationID(env_id)
        parameters = [i for i in parameters if i['application'] == applicationID]
        
        return parameters
    
    
    def parameter_list(self, 
                       env_id:int, 
                       params:dict = {}):
        """
        Desc:
            Return a list of parameters that in the game
        
        Issue:
            Inputing applicationID in params doesn't work, 
            thus query all parameters under the api key and extract right game parameters later.
            
        Output
            pd.DataFrame
        """
        parameters = self._parameter_spec(env_id=env_id, params=params)
        
        if parameters is None:
            return None
        
        result = []
        for parameter in parameters:
            param = [[parameter.get(j) for j in ['id', 'name', 'type', 'description'] + self.metricsField]]
            param = pd.DataFrame(param, columns = ['id', 'name', 'type', 'description'] + self.metricsField)
            result.append(param)
        result = pd.concat(result, axis=0).reset_index(drop=True)
        result.rename(columns={'id': 'ParameterID'}, inplace=True)
        result.insert(0, 'ApplicationID', self._get_applicationID(env_id))
        
        return result
    
    
    def parameter_search(self, 
                         env_id:int, 
                         param_name:int, 
                         params:dict = {}):
        """
        Desc
            Searching a parameter in the application.
        
        Input
            params: example
                {"limit": int, 
                 "page": int,
                 "applicationID": int,
                 "required": bool,
                 "type": str [STRING, INTEGER, BOOLEAN, TIMESTAMP, FLOAT]
                }
                https://app.swaggerhub.com/apis-docs/deltaDNA/Engage/1.0.2#/
        
        Output
            Parameter detail, pd.DataFrame
        """
        col_used = ['application', 'id', 'name', 'description', 'type'] + self.metricsField
        parameters = self._parameter_spec(env_id=env_id, params=params)
        
        if parameters is None:
            return None
        
        parameter = [[i.get(j) for j in col_used] 
                     for i in parameters if i['name']==param_name]
        parameter = pd.DataFrame(parameter, columns = col_used)
        parameter.columns = ['Game ID', 'Parameter ID', 'Parameter Name', 'Description', 'Type'] + self.metricsField
        parameter.insert(0, 'Environemnt ID', env_id)
        
        return parameter
    
    
    def add_event(self, 
                  env_id:int, 
                  event_name:str, 
                  description:str):
        """
        Desc
            Adding a single event to the environment.
        
        Output
            Event detailed information
        """
        url = 'https://api.deltadna.net/api/events/v1/events'
        
        body = {
            'name': event_name,
            'description': description,
            'environment': env_id
        }
        
        ddna = requests.post(url, headers=self._header, json=body)
        if ddna.status_code != 200:
            print('Failed creating the event, error code: ', ddna.status_code)
            msg = json.loads(ddna.text)
            msg['parameters'] = 'None'
        else:
            print('Successfully created the event.', ddna.status_code)
            msg = json.loads(ddna.text)
            msg['parameters'] = 'Not showing here.'
        
        return msg
    
    
    def add_parameter(self, 
                      env_id:int,
                      param_name:str,
                      param_desc:str, 
                      param_type:str, 
                      param_format:str,
                      calculatingMetricFirst:bool = None,
                      calculatingMetricLast:bool = None,
                      calculatingMetricCount:bool = None,
                      calculatingMetricMin:bool = None,
                      calculatingMetricMax:bool = None,
                      calculatingMetricSum:bool = None
                     ):
        """
        Desc
            Adding a single parameter to the application
        
        Return
            Parameter detailed information
        
        Note
            JSON input data has to use double quote, and numbers have to be Python native int.
        """
        url = 'https://api.deltadna.net/api/events/v1/event-parameters'
        param_id = self.event_details(env_id)['parameterID'].max() + 1
        app_id = self._game_list[self._game_list['Environment ID']==env_id]['Application ID'].item()
        app_name = self._game_list[self._game_list['Environment ID']==env_id]['Game Name'].item()
        body = {
            "id": 1020234,
            "name": param_name,
            "description": param_desc,
            "application": app_id,
            "type": param_type,
            "format": param_format,
            "calculatingMetricFirst": calculatingMetricFirst,
            "calculatingMetricLast": calculatingMetricLast,
            "calculatingMetricCount": calculatingMetricCount,
            "calculatingMetricMin": calculatingMetricMin,
            "calculatingMetricMax": calculatingMetricMax,
            "calculatingMetricSum": calculatingMetricSum
        }
        
        ddna = requests.post(url, headers=self._header, json=body)
        if ddna.status_code != 200:
            print('Failed to create the parameter. Error code: ', ddna.status_code)
            print(json.loads(ddna.text)['title'])
            body.update({'isCreated': False})
            return body
        else:
            print('Successfully created the parameter.')
            response = json.loads(ddna.text)
            print('[', response['name'], ']', 'created in', response['application'], 
                  '[', app_name, ']', 'with type', 
                  response['type'], 'and format', response['format'], '.')
            print('calculatingMetricFirst:', response.get('calculatingMetricFirst'))
            print('calculatingMetricLast:', response.get('calculatingMetricLast'))
            print('calculatingMetricCount:', response.get('calculatingMetricCount'))
            print('calculatingMetricMin:', response.get('calculatingMetricMin'))
            print('calculatingMetricMax:', response.get('calculatingMetricMax'))
            print('calculatingMetricSum:', response.get('calculatingMetricSum'))
            print('\n')
            body.update({'isCreated': True})
            return body
    
    
    def _param_to_event(self, 
                        env_id:int, 
                        param_name:str, 
                        event_name:str, 
                        add_remove:str,
                        required:bool
                       ):
        """
        Desc
            Called by add/remove parameter to/from event function.
        
        Output
            A dict of parameter and event information.
        """
        prep = {'add': 'to', 'remove': 'from'}
        body = {'add': {"required": required}, 'remove': {}}
        
        event_id = [i for i in self.event_list(env_id) if i[0]==event_name]
        param_id = self.parameter_search(env_id, param_name)['Parameter ID']
        if len(event_id) != 1:
            print('Event is not found.')
            return None
        elif param_id.shape[0] < 1:
            print('Parameter is not found.')
            return None
        else:
            event_id = event_id[0][1]
            param_id = param_id.item()
        
        url = f'https://api.deltadna.net/api/events/v1/events/{event_id}/{add_remove}/{param_id}'
        ddna = requests.post(url, headers=self._header, json=body[add_remove])
        if ddna.status_code != 200:
            print(f'Failed to {add_remove} the parameter [ {param_name} ] {prep[add_remove]} '
                  f'the event [ {event_name} ]')
            return json.loads(ddna.text)['title']
        
        if add_remove == 'add':
            result = json.loads(ddna.text)['parameters']
            result = [i for i in result if i['name']==param_name][0]
            result.update({'From Event': event_name})
            return result
        else:
            result = {'name': param_name, 'From Event': event_name}
            return result
    
    
    def add_param_to_event(self, 
                           env_id:int, 
                           param_name:str, 
                           event_name:str, 
                           required:bool = False):
        """
        Desc
            Adding a parameter to an event.
            
        Output
            A dict describing the parameter detail and event name.
        
        Note
            Error will not be raised if the parameter already exists in the event.
        """
        result = self._param_to_event(env_id = env_id, 
                                      param_name = param_name, 
                                      event_name = event_name, 
                                      add_remove = 'add', 
                                      required = required
                                     )
        
        if result is None:
            return None
        else:
            print(f'[ {param_name} ] is added to event [ {event_name} ].')
            return result
    
    
    def remove_param_from_event(self, 
                                env_id:int, 
                                param_name:str, 
                                event_name:str):
        """
        Desc
            Removing a parameter from an event.
        
        Output
            A dict of parameter name and event name.
        """
        result = self._param_to_event(env_id = env_id, 
                                      param_name = param_name, 
                                      event_name = event_name, 
                                      add_remove = 'remove', 
                                      required = False)
        if result is None:
            return None
        else:
            print(f'[ {param_name} ] is removed from event [ {event_name} ].')
            return result