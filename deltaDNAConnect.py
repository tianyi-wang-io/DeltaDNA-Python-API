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
        About Application and Environment IDs:
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
    
    
    def _event_spec(self, env_id:int):
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
        events = self._event_spec(env_id=env_id)
        if events is None:
            return None
        
        events = [i['name'] for i in events]
            
        return events
    
    
    def event_details(self, 
                      env_id:int):
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
        result.rename(columns = {'name': 'Event Name', 'id': 'eventID'}, inplace=True)
        
        return result
    
    
    def _get_applicationID(self, env_id):
        """
        Desc:
            Getting applicationID based on environmentID
        """
        return self._game_list[self._game_list['Environment ID']==env_id]['Application ID'][0]
    
    
    def _parameter_spec(self, env_id:int, params:dict):
        """
        
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
        
        """
        parameters = self._parameter_spec(env_id=env_id, params=params)
        
        if parameters is None:
            return None
        
        result = []
        for parameter in parameters:
            param = [[parameter.get(j) for j in ['name', 'type', 'description'] + self.metricsField]]
            param = pd.DataFrame(param, columns = ['name', 'type', 'description'] + self.metricsField)
            result.append(param)
        result = pd.concat(result, axis=0).reset_index(drop=True)
        result.insert(0, 'Environment ID', env_id)
        
        return result
    
    
    def parameter_search(self, env_id:int, param_name:int, params:dict = {}):
        """
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
    
    
    def add_event(self, env_id:int, event_name:str, description:str):
        """
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
        