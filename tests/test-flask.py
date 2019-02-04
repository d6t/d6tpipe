import os
import requests
import json
import copy

srv = "http://127.0.0.1:5000"
os.environ['D6TFLASKTOKEN']='securetoken'
cfg_remote = {
    "name": "test",
    "protocol": "s3",
    "location": "test-augvest-20180719"
}
cfg_pipe = {
    "name": "test",
    "remote": "test"
}

def run(url,method='get',body=None):
    fun = getattr(requests,method)
    if method in ['post','put','patch']:
        return fun(srv+url, json=body, headers={'Authorization': 'Token securetoken'}).json()
    else:
        return fun(srv+url).json()

def buildurl(l):
    return '/'.join(l)

class TestFlask(object):

    def setup_method(self, method):
        run('/wipeall','post')
        assert run('/remotes')==[]
        # assert run('/pipes')==[]

    def test_flask(self):
        assert 'description' in run('/')
        def helper1(urlbase,dict_):
            assert run(urlbase)==[]
            assert run(urlbase,'post',dict_)==dict_
            assert run(urlbase)[0]==dict_
            assert run(urlbase+'/'+dict_['name'])==dict_
            dict_['new']='new'
            assert run(urlbase+'/'+dict_['name'],'put',dict_)==dict_
            assert run(urlbase+'/'+dict_['name'])==dict_

        helper1('/remotes',cfg_remote)
        helper1('/pipes',cfg_pipe)

        run('/pipes/'+cfg_pipe['name']+'/files/','post',['a.csv','b.csv'])
        assert run('/pipes/'+cfg_pipe['name']+'/files/')==['a.csv','b.csv']

