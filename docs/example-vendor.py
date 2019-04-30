import d6tpipe

#****************************
# vendor
#****************************

api = d6tpipe.APIClient(profile='d6tdev')

d6tpipe.upsert_pipe(api,{'name':'demo-vendor'})

pipe = d6tpipe.Pipe(api, 'demo-vendor')
print(pipe.settings['options']['remotepath'])

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

#****************************
# client permissions
#****************************
d6tpipe.api.ConfigManager(profile='demo').init() # use demo account
api2 = d6tpipe.api.APIClient(profile='demo')
api2.login('demo','demo123')

# trial client subscription: all you can eat
d6tpipe.upsert_permissions(api,'demo-vendor',{'username':'demo','role':'read','expiration_date':'2019-12-31'}) # auto expire at end of trial
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
# client permissions
#****************************
files = pipe2.pull()
files = ['month2.csv']
data_processed = futures_compute(files,'methodolgy')
data_processed.to_csv(pipe_out.dirpath/'month2.csv')
pipe_out.pull()
