import d6tpipe

cfg_local = False
#****************************
# vendor
#****************************

if not cfg_local:
    api = d6tpipe.APIClient(profile='d6tdev')
else:
    d6tpipe.api.ConfigManager(profile='d6tdev-local').update({'token':'a0cefc11ec9b2d6a44f7e360d6ea36d494679f8c'})
    api = d6tpipe.APIClient(profile='d6tdev-local')#,token=None) api.login()

d6tpipe.upsert_pipe(api,{'name':'demo-vendor'})

pipe = d6tpipe.Pipe(api, 'demo-vendor')
print(pipe.settings['options']['remotepath'])
print(pipe._get_credentials())

import pandas as pd
df = pd.DataFrame({'date':range(10),'data':range(10)})
df.to_csv(pipe.dirpath/'monthly-201901.csv')
df.to_csv(pipe.dirpath/'daily-201901.csv')

pipe.setmode('all')
pipe.push_preview()
pipe.push()

# create subscription levels
d6tpipe.upsert_pipe(api,{'name':'demo-vendor-daily','parent':'demo-vendor','options':{'include':'*daily*.csv'}})
d6tpipe.upsert_pipe(api,{'name':'demo-vendor-monthly','parent':'demo-vendor','options':{'include':'*monthly*.csv'}})

# trial client subscription: all you can eat
d6tpipe.upsert_permissions(api,'demo-vendor',{'username':'demo','role':'read','expiration_date':'2019-12-31'}) # auto expire at end of trial

#****************************
# client connects
#****************************
if not cfg_local:
    d6tpipe.api.ConfigManager(profile='demo').init() # use demo account
    api2 = d6tpipe.api.APIClient(profile='demo')
    api2.login('demo','demo123')
else:
    d6tpipe.api.ConfigManager(profile='demo-local').update({'token':'69337d229e1e35677623341973a927518a6abc7c'})
    api2 = d6tpipe.api.APIClient(profile='demo-local')#,token=None)

pipe2 = d6tpipe.Pipe(api2, 'demo-vendor')
pipe2.scan_remote()
pipe2.pull()

# trial ends, client subscription: monthly only
d6tpipe.upsert_permissions(api,'demo-vendor',{'username':'demo','role':'revoke'})
try:
    pipe2 = d6tpipe.Pipe(api2, 'demo-vendor') # fails
except Exception as e:
    print(e)
d6tpipe.upsert_permissions(api,'demo-vendor-monthly',{'username':'demo','role':'read'})
try:
    pipe2 = d6tpipe.Pipe(api2, 'demo-vendor') # fails
except Exception as e:
    print(e)
pipe2 = d6tpipe.Pipe(api2, 'demo-vendor-monthly')
pipe2.scan_remote()

#****************************
# vendor analytics
#****************************
pipe.cnxnpipe.analytics.get()


#****************************
# REST CLI
#****************************

c = pipe._get_credentials() # pass write=True for write credentials
print(f'SET AWS_ACCESS_KEY_ID={c["aws_access_key_id"]}')
print(f'SET AWS_SECRET_ACCESS_KEY={c["aws_secret_access_key"]}')
print(f'SET AWS_SESSION_TOKEN={c["aws_session_token"]}')

print(f'export AWS_ACCESS_KEY_ID={c["aws_access_key_id"]}')
print(f'export AWS_SECRET_ACCESS_KEY={c["aws_secret_access_key"]}')
print(f'export AWS_SESSION_TOKEN={c["aws_session_token"]}')

print(f"aws s3 ls {pipe.settings['options']['remotepath']}")
