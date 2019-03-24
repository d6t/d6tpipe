import d6tpipe


d6tpipe.api.ConfigManager(profile='cloud').init({'server':'http://api.databolt.tech'})

api = d6tpipe.APIClient(profile='utest-local')
# d6tpipe.api.ConfigManager(profile='utest-local').update({'token':None})
# api.register('utest-local','a@b.com','utest-local')
# api.login('utest-dev','utest-devpwd')

"utest-d6tdev"
