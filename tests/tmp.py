import d6tpipe
api = d6tpipe.APIClient(profile='utest-local')
api = d6tpipe.APIClient(profile='utest-d6tdev')
# d6tpipe.api.ConfigManager(profile='utest-local').update({'token':None})
# api = d6tpipe.APIClient(profile='utest-local')
# api.register('utest-local','a@b.com','utest-local')
# api.login('utest-dev','utest-devpwd')

api.list_pipes(names_only=False)
pipe = d6tpipe.Pipe(api,'nyu-2019Q1-backtest',mode='all')

pipe.settings

pipe.scan_remote()
pipe.pull()

quit()

d6tpipe.api.ConfigManager(profile='demo-prod').init()
api.register('demo', 'demo@databolt.tech', 'demo123')
api = d6tpipe.APIClient(profile='demo-prod')
api.list_pipes()
pipe = d6tpipe.Pipe(api,'intro-stat-learning')
pipe.pull()

"utest-d6tdev"

cfg_name = 'utest-d6tfree'
api.cnxn.pipes._(cfg_name).get()
api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role':'read'})


pipe = d6tpipe.Pipe(api,'utest-d6tfree-tmp')
pipe.pull()

cfg_name = 'test-ftp'
api.cnxn.pipes._(cfg_name).get()
api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role':'read'})

quit()
# pipe.remove_orphans(['test.csv'],dryrun=False)
pipe.remove_orphans('both', dryrun=True)['remote']
pipe.list_remote()
pipe.pull()
quit()
pipe._empty_local(False,True)

cfg_settings_parent = {
    "name": 'utest-local',
    "protocol": "s3",
    "location": "test-augvest-20180719",
}
api.cnxn.pipes.post(request_body=cfg_settings_parent)
api.cnxn.pipes._(cfg_settings_parent['name']).get()
# api.cnxn.pipes._(cfg_settings_parent['name']).delete()
d=api.cnxn.pipes._(cfg_settings_parent['name']).get()[1]
d['schema']
d['cache']=='OrderedDict()'

cfgjson = d6tpipe.utils.loadjson('tests/.creds-test.json')

cfg_profile = 'utest-dev'
cfg_parent_name = cfg_profile+'-remote'+'-tmp'
cfg_pipe_name = cfg_profile+'-pipe'+'-tmp'

cfg_settings_parent = {
    "name": cfg_parent_name,
    "protocol": "s3",
    "location": "test-augvest-20180719",
    "credentials": cfgjson['aws_augvest'],
}

cfg_settings_pipe = {
    "name": cfg_pipe_name,
    "parent": cfg_parent_name,
    "options": {
        "dir": "vendorX/",
        "include": "*.csv"
    },
    'schema': {'pandas': {'sep': ','} }
}

# api.cnxn.pipes._(cfg_settings_parent['name']).delete()
# api.cnxn.pipes._(cfg_settings_pipe['name']).delete()

api.cnxn.pipes.post(request_body=cfg_settings_parent)
api.cnxn.pipes.post(request_body=cfg_settings_pipe)
api.cnxn.pipes._(cfg_settings_parent['name']).get()
api.cnxn.pipes._(cfg_settings_pipe['name']).get()

api.cnxn.pipes._(cfg_settings_pipe['name']).credentials.get(query_params={'role':'write'})


settings = {'name':'utest-d6tfree-tmp','protocol':'d6tfree'}
# d6tpipe.upsert_pipe(api, settings)
api.cnxn.pipes._(settings['name']).get()
api.cnxn.pipes._(settings['name']).credentials.get(query_params={'role':'read'})

settingschild = {'name':'utest-d6tfree-tmp-child','parent':settings['name'],'options':{'dir':'test'}}
# d6tpipe.upsert_pipe(api, settingschild)
api.cnxn.pipes._(settingschild['name']).get()
api.cnxn.pipes._(settingschild['name']).credentials.get(query_params={'role':'read'})


# api.cnxn.pipes._(cfg_settings_pipe['name']).permissions.post(request_body={'username':'test','role':'read'})

quit()

import d6tpipe
api = d6tpipe.api.APIClient()
import importlib
importlib.reload(d6tpipe.api)
importlib.reload(d6tpipe.pipe)
remotename = 'test-d6tfree'
settings = {'name':remotename,'protocol':'d6tfree'}
d6tpipe.api.create_or_update(api.cnxn.remotes,settings)
api.cnxn.remotes._(remotename).get()

pipe = d6tpipe.pipe.Pipe(api,remotename)

import pandas as pd
pd.DataFrame({'a':range(10)}).to_csv(pipe.dirpath/'test2.csv')
pipe.push_preview()

quit()

import pandas as pd
import os
import logging
# logging.getLogger().setLevel(logging.DEBUG)

from dotenv import load_dotenv
load_dotenv()

import d6tpipe
import d6tpipe.api
import d6tpipe.pipe

import importlib
importlib.reload(d6tpipe.api)
importlib.reload(d6tpipe.pipe)
d6tpipe.api.ConfigManager().init({'server':'http://192.168.33.10:5000'},reset=True)

# api = d6tpipe.api.APILocal()
api = d6tpipe.api.APIClient()
token = api.register('testusr','email@test.com','testpwd')
d6tpipe.api.ConfigManager().update({'token':token})



import d6tpipe
importlib.reload(d6tpipe.api)
d6tpipe.api.ConfigManager(profile='user2').init({'server':'http://192.168.33.10:5000'},reset=True)
api = d6tpipe.api.APIClient(profile='user2')
api.register('testusr2','email2@test.com','testpwd')
api.cnxn.remotes.get()


import d6tpipe
d6tpipe.api.ConfigManager(profile='heroku-staging').init({'server':'http://192.168.33.10:5000'}, reset=True)
api = d6tpipe.api.APIClient(profile='heroku-staging')
token = api.register('testusr','email@test.com','testpwd')
d6tpipe.api.ConfigManager(profile='heroku-staging').update({'token':token})
api = d6tpipe.api.APIClient(profile='heroku-staging')

importlib.reload(d6tpipe.pipe)
pipe = d6tpipe.pipe.Pipe(api,'test-s3',mode='all')
pipe.remote_prefix


import importlib
import d6tpipe
api = d6tpipe.api.APILocal()
importlib.reload(d6tpipe.utils)


d6tpipe.api.create_or_update_from_json(api.cnxn.remotes, api.repopath / '.creds.json', 'remotes-test-s3')
d6tpipe.api.create_or_update_from_json(api.cnxn.remotes, api.repopath / '.creds.json', 'remotes-test-s3-2')
d6tpipe.api.create_or_update_from_json(api.cnxn.pipes, api.repopath / '.creds.json', 'pipes-test-s3')
d6tpipe.api.create_or_update_from_json(api.cnxn.pipes, api.repopath / '.creds.json', 'pipes-test-s3-2')
d6tpipe.api.create_or_update_from_json(api.cnxn.remotes, api.repopath / '.creds.json', 'remotes-test-ftp')
d6tpipe.api.create_or_update_from_json(api.cnxn.pipes, api.repopath / '.creds.json', 'pipes-test-ftp')

settings = d6tpipe.api.loadjson(api.repopath / '.creds.json')['remotes-test-s3']
api.cnxn.remotes.post(request_body=settings)

api.cnxn.remotes._('test-s3').get()
api.cnxn.pipes._('test-s3').get()

api.cnxn.pipes._('test-ftp').get()


settings = d6tpipe.api.loadjson(api.repopath / '.creds.json')['remotes-test-s3-2']
d6tpipe.api.create_or_update(api.cnxn.remotes, settings)
api.cnxn.remotes.post(request_body=settings)




pipe = d6tpipe.pipe.Pipe(api,'test-s3')
pipe.scan_remote()

pipe = d6tpipe.pipe.Pipe(api,'test-ftp')
pipe.scan_remote()

import d6tpipe.api
import importlib
importlib.reload(d6tpipe.api)
d6tpipe.api.ConfigManager(profile='user2').init({'server':'http://192.168.33.10:5000'},reset=True)

# api = d6tpipe.api.APILocal()
api = d6tpipe.api.APIClient(profile='user2')
token = api.register('testusr2','email2@test.com','testpwd')
d6tpipe.api.ConfigManager(profile='user2').update({'token':token})

api2 = d6tpipe.api.APIClient(profile='user2')

api = d6tpipe.api.APIClient()

api.cnxn.remotes._('test-s3').get()
api.cnxn.pipes._('test-s3').get()

api2.cnxn.remotes._('test-s3').get()
api2.cnxn.remotes.get()
api2.cnxn.pipes._('test-s3').get()
api.cnxn.pipes._('test-s3').get()

api.cnxn.remotes._('test-s3').get()
d6tpipe.api.create_or_update_permissions(api.cnxn.remotes._("test-s3").permissions, {"user":"testusr2","role":"write"})



d6tpipe.api.create_or_update_from_json(api2.cnxn.remotes, api.repopath / '.creds.json', 'remotes-test-s3')

settings = d6tpipe.api.loadjson(api.repopath / '.creds.json')['remotes-test-s3']
settings['name']='test-s3-2'
api2.cnxn.remotes.post(request_body=settings)
api2.cnxn.remotes._("test-s3-2").permissions.post({"user":"testusr","role":"read"})

settings = d6tpipe.api.loadjson(api.repopath / '.creds.json')['pipes-test-s3']
settings['name']='test-s3-2'
api2.cnxn.pipes.post(request_body=settings)
api2.cnxn.pipes._("test-s3-2").permissions.post({"user":"testusr","role":"read"})



import d6tpipe
d6tpipe.api.ConfigManager().init() # just once

api = d6tpipe.api.APIClient()
pipe = d6tpipe.pipe.Pipe(api,'intro-stat-learning')
pipe.pull()
pipe.push()

d6tpipe.api.ConfigManager(profile='user2').init({'filerepo':r'C:\Data\d6tpipe-files2'}) # just once
api2 = d6tpipe.api.APIClient(profile='user2')
# api2.forgotToken('d6tdev2','d6tdev2')

import importlib
importlib.reload(d6tpipe.pipe)
pipe2 = d6tpipe.pipe.Pipe(api2,'intro-stat-learning')
pipe2.pull_preview()


