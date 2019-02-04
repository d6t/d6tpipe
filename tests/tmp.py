import d6tpipe
api = d6tpipe.APIClient()#profile='utest-d6tdev')
api.list_remotes()

api2 = d6tpipe.APIClient(profile='citynorman')
api2.list_remotes()

pipe = d6tpipe.pipe.Pipe(api,'intro-stat-learning')
pipe.pull()

import pandas as pd
df = pd.read_csv(pipe.dirpath/'Advertising.csv')

df = pd.read_csv(pipe.dirpath / 'Advertising.csv', **pipe.readparams['pandas'])
print(df.head())

import sklearn.preprocessing
df = sklearn.preprocessing.scale(df)
df.to_csv(pipe.dirpath / 'Advertising.csv')  # pipe.dirpath points to local pipe folder

import dask.dataframe as dd
files = pipe.files(include='Advertising*.csv')
import dask.dataframe as dd

files = pipe.files(include='Advertising*.csv')
ddf = dd.read_csv(files, **pipe.readparams['dask'])
print(ddf.head())

df = pd.read_csv(pipe.files(include='*.csv')[-1])

pd.read_csv(pipe.dirpath/'Income1.csv').head()
pd.read_csv(pipe.dirpath/'Income2.csv').head()

for iuser in ['sn2793','yc3526','zeana10']:
    settings = {"user":iuser,"role":"read"} # read, write, admin
    d6tpipe.create_or_update_permissions(api2, 'nyu-2019Q1-backtest', settings)


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


