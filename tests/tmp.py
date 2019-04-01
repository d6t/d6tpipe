import d6tpipe
api = d6tpipe.APIClient(profile='utest-local')
api = d6tpipe.APIClient(profile='utest-d6tdev')

api = d6tpipe.APIClient(profile='utest-dev', filecfg='~/d6tpipe/cfg-utest-dev.json')
api2 = d6tpipe.APIClient(profile='utest-dev2', filecfg='~/d6tpipe/cfg-utest-dev.json')
