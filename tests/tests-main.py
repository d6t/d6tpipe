import glob
import numpy as np
import pandas as pd
import os
import pytest
from collections import Counter
import shutil
import copy
import warnings
from pathlib import Path

from .conftest_common import *

import d6tpipe.api
import d6tpipe.pipe
from d6tpipe.http_client.exceptions import APIError
from d6tpipe.exceptions import *
from jwt.exceptions import InvalidSignatureError
from d6tpipe.pipe import _filenames

import d6tcollect
d6tcollect.submit = False

'''
todo:
interact with non-existing remotes/pipes
bucket names and folders /. eg remote s3://name/vendorX vs s3://name/vendorX/ with subdir /some/path vs some/path vs /some/path/
=> use import import_files to subdir function 
ftp tests
flask api tests
'''

# ************************************
# config
# ************************************
cfg_test_param_local = [True]

cfg_server = 'http://192.168.33.10:5000'
cfg_server = 'https://d6tpipe-staging-demo.herokuapp.com'

scenario0 = ('blank', {})
scenario1 = ('local', {'testcfg':{'local': True, 'encrypt':False}})
scenario2 = ('remote', {'testcfg':{'encrypt':False}})
scenario3 = ('remote-encrypt', {'testcfg':{}})
scenario4 = ('heroku-staging', {'testcfg':{'server':'https://d6tpipe-staging-demo.herokuapp.com','encrypt':False}})
scenario5 = ('heroku-prod', {'testcfg':{'server':'https://pipe.databolt.tech'}})
# scenario5 = ('heroku-prod', {'testcfg':{'server':'https://d6tpipe-prod-live.herokuapp.com'}})

# ************************************
# setup
# ************************************
class TestMain(object):
    # scenarios = [scenario1, scenario2, scenario3]
    # scenarios = [scenario1]#[scenario1, scenario2, scenario3]
    scenarios = [scenario4]
    # scenarios = [scenario4, scenario5]

    @pytest.fixture(scope="class")
    def cleanup(self, testcfg):
        # make sure nothing exists before starting tests

        cleaner(getapi,cfg_usr)
        cleaner(getapi2,cfg_usr2)

        assert not os.path.exists(cfg_cfgfname2)

        d6tcollect.host = testcfg.get('server',cfg_server)

        # user1
        d6tpipe.api.ConfigManager(profile=cfg_profile,filecfg=cfg_cfgfname).init({'server':testcfg.get('server',cfg_server)})
        # user2
        d6tpipe.api.ConfigManager(profile=cfg_profile2,filecfg=cfg_cfgfname).init({'server':testcfg.get('server',cfg_server)})

        yield True

        # tear down after tests complete
        cleaner(getapi,cfg_usr)
        cleaner(getapi2,cfg_usr2)

    def test_cleanup(self, cleanup, testcfg):

        # user1
        assert os.path.exists(cfg_cfgfname2)

        cfgm = d6tpipe.api.ConfigManager(profile=cfg_profile,filecfg=cfg_cfgfname)
        cfg = cfgm.load()
        assert 'filerepo' in cfg and 'filedb' in cfg
        assert 'token' not in cfg

    @pytest.fixture(scope="class")
    def signup(self, testcfg):
        if not testcfg.get('local',False):

            # potential unregister
            unregister(getapi,cfg_usr)
            unregister(getapi2,cfg_usr2)

            # register
            token = getapi().register(cfg_usr, cfg_usr+'@domain.com', cfg_usrpwd)

            # getconfig(cfg_profile).update({'token':token})

            # user2
            token2 = getapi2().register(cfg_usr2, cfg_usr2+'@domain.com', cfg_usrpwd)
            # getconfig(cfg_profile2).update({'token':token2})

        else:
            token=''; token2='';

        yield token, token2

        unregister(getapi,cfg_usr)
        unregister(getapi2,cfg_usr2)


    def test_apisignup(self, cleanup, signup, testcfg):
        if testcfg.get('local',False) or testcfg.get('encrypt',True):  # don't run tests
            return True

        token, token2 = signup

        # can't register again
        with pytest.warns(UserWarning):
            getapi().register(cfg_usr, cfg_usr+'@domain.com', cfg_usrpwd)
        with pytest.warns(UserWarning):
            getapi2().register('xxx', cfg_usr+'@domain.com', cfg_usrpwd)

        ttoken = getapi().forgotToken(cfg_usr, cfg_usrpwd)
        assert ttoken==token

        # token in config
        cfgm = d6tpipe.api.ConfigManager(profile=cfg_profile,filecfg=cfg_cfgfname)
        cfg = cfgm.load()
        assert cfg['token']==token

        api = getapi()
        assert api.cfg_profile['token']==token
        api2 = getapi2()
        assert api2.cfg_profile['token']==token2

        # cant unregister another user
        with pytest.raises(APIError, match='can only unregister yourself'):
            api._unregister(cfg_usr2)


    @pytest.fixture(scope="class")
    def remoteinit(self, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_remote
        if testcfg.get('encrypt',True):
            settings = api.encode(settings)

        # check clean
        r, d = api.cnxn.remotes.get()
        assert d==[]

        # create
        response, data = d6tpipe.api.create_or_update(api.cnxn.remotes, settings)
        if testcfg.get('local',False):
            assert response.status_code == 200
        else:
            assert response.status_code == 201
        yield True

        r, d = api.cnxn.remotes._(cfg_remote).delete()
        assert r.status_code==204



    def test_apiremotes(self, cleanup, signup, remoteinit, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_remote

        # no duplicate names
        with pytest.raises(APIError, match='unique'):
            response, data = api.cnxn.remotes.post(request_body=settings)
        # todo: another user creates

        if not testcfg.get('local',False):
            # only alphanumeric
            with pytest.raises(APIError, match='alphanumeric'):
                settingsbad = settings.copy()
                settingsbad['name']='$invalid'
                api.cnxn.remotes.post(request_body=settingsbad)

        r, d = api.cnxn.remotes._(cfg_remote).get()
        if testcfg.get('encrypt',True):
            # check can't decode with wrong key
            k = api.key; api.key = 'wrong';
            with pytest.raises(InvalidSignatureError):
                api.decode(d)
            api.key = k

            d = api.decode(d)

        assert d['name'] == cfg_settings_remote['name']
        assert d['readCredentials'] == cfg_settings_remote['readCredentials']

        # create and delete
        r, d = api.cnxn.remotes.get()
        assert len(d)==1
        settings2 = settings.copy()
        settings2['name'] = cfg_remote+'2'
        response, data = d6tpipe.api.create_or_update(api.cnxn.remotes, settings2)
        r, d = api.cnxn.remotes.get()
        assert len(d)==2
        r, d = api.cnxn.remotes._(settings2['name']).delete()
        assert r.status_code == 204
        r, d = api.cnxn.remotes.get()
        assert len(d)==1

        if not testcfg.get('local',False):
            api2 = getapi2()
            with pytest.raises(APIError, match='403'):
                r, d = api2.cnxn.remotes._(cfg_remote).get()

        r, d = api.cnxn.pipes.get()
        assert d==[]


    @pytest.fixture()
    def pipeinit(self, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_pipe

        response, data = d6tpipe.api.create_or_update(api.cnxn.pipes, settings)
        yield True

        r, d = api.cnxn.pipes._(cfg_pipe).delete()
        assert r.status_code == 204

    def test_apipipes(self, cleanup, signup, remoteinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))

        r, d = api.cnxn.pipes._(cfg_pipe).get()
        assert d['remote'] == cfg_remote
        assert d['settings'] == cfg_settings_pipe['settings']

        # todo: check non-existing remote
        # todo: delete remote, pipes deleted

        # check permissions
        if not testcfg.get('local',False):
            api2 = getapi2()

            r, d = api2.cnxn.pipes.get()
            assert d == []

            with pytest.raises(APIError, match='404'): # todo: 404 vs 403 error?
                r, d = api2.cnxn.remotes._(cfg_pipe).get()

    def test_permissions(self, cleanup, signup, remoteinit, pipeinit, testcfg):
        if not testcfg.get('local',False):
            api = getapi(testcfg.get('local',False))
            api2 = getapi2(testcfg.get('local',False))
            
            def helperowner():
                helper(api, nremotes=1, npipes=1, iswrite=True, isadmin=True)

            def helper(_api, nremotes, npipes, iswrite, isadmin):
                rr = _api.cnxn.remotes.get()[1]
                rp = _api.cnxn.pipes.get()[1]
                assert len(rr)==nremotes
                assert len(rp)==npipes
                if nremotes==0:
                    with pytest.raises(APIError, match='403'):
                        _api.cnxn.remotes._(cfg_remote).get()
                else:                    
                    rrd = _api.cnxn.remotes._(cfg_remote).get()[1]
                    assert ('writeCredentials' in rrd)==iswrite
                    if not isadmin:
                        with pytest.raises(APIError, match='403'):
                            _api.cnxn.remotes._(cfg_remote).put(request_body={'bad':'request'})
                if npipes==0:
                    with pytest.raises(APIError, match='403'):
                        _api.cnxn.pipes._(cfg_pipe).get()
                else:
                    rpd = _api.cnxn.pipes._(cfg_pipe).get()[1]
                    if not isadmin:
                        with pytest.raises(APIError, match='403'):
                            _api.cnxn.pipes._(cfg_pipe).put(request_body={'bad': 'request'})
                if isadmin:
                    # put
                    pass
                else:
                    # catch put error
                    pass


            helperowner()
            helper(api2, nremotes=0, npipes=0, iswrite=False, isadmin=False)
            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": cfg_usr2, "role": "read"})
            helper(api2, nremotes=1, npipes=1, iswrite=False, isadmin=False)
            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": cfg_usr2, "role": "write"})
            helper(api2, nremotes=1, npipes=1, iswrite=True, isadmin=False)
            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": cfg_usr2, "role": "revoke"})
            helper(api2, nremotes=0, npipes=0, iswrite=False, isadmin=False)

            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": "public", "role": "read"})
            helper(api2, nremotes=1, npipes=1, iswrite=False, isadmin=False)
            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": "public", "role": "write"}) # public write disabled for now
            helper(api2, nremotes=0, npipes=0, iswrite=False, isadmin=False)
            api.cnxn.remotes._(cfg_remote).permissions.post(request_body={"user": "public", "role": "revoke"})
            helper(api2, nremotes=0, npipes=0, iswrite=False, isadmin=False)


    def test_pipes_pull(self, cleanup, signup, remoteinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api)

        cfg_chk_crc = ['8a9782e9efa8befa9752045ca506a62e',
         '5fe579d6b71031dad399e8d4ea82820b',
         '4c7da169df85253d7ff737dde1e7400b',
         'ca62a122993494e763fd1676cce95e76']

        r, d = pipe.scan_remote()
        assert _filenames(d) == cfg_filenames_chk
        assert [o['crc'] for o in d]==cfg_chk_crc

        assert pipe.pull_preview() == cfg_filenames_chk
        assert pipe.pull() == cfg_filenames_chk
        assert pipe.pull_preview() == []

        assert pipe.filenames() == cfg_filenames_chk
        assert pipe.files() == [Path(pipe.dirpath)/f for f in pipe.filenames()]
        assert pipe.files(aspathlib=False) == [str(Path(pipe.dirpath)/f) for f in pipe.filenames()]

        pipe = getpipe(api, chk_empty=False, mode='all')
        assert pipe.pull_preview() == cfg_filenames_chk

        # PipeLocal
        pipelocal = d6tpipe.PipeLocal(pipe.pipe_name,profile=cfg_profile, filecfg=cfg_cfgfname)
        assert pipelocal.filenames() == cfg_filenames_chk
        assert pipelocal.scan_local_filenames() == cfg_filenames_chk

        # permissions
        if not testcfg.get('local',False):
            api2 = getapi2(testcfg.get('local', False))
            with pytest.raises(APIError, match='403'):
                pipe2 = getpipe(api2, name=cfg_pipe, mode='all')
                pipe2.pull()

            settings = {"user": cfg_usr2, "role": "read"}
            d6tpipe.create_or_update_permissions(api, cfg_remote, settings)

            pipe2 = getpipe(api2, name=cfg_pipe, mode='all')
            assert pipe2.pull()==cfg_filenames_chk

        # cleanup
        pipe._empty_local(confirm=False,delete_all=True)

    def test_pipes_push(self, cleanup, signup, remoteinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api)
        with pytest.raises(PushError):
            pipe.push_preview()
        pipe.pull()

        # push works
        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert pipe.push_preview()==[cfg_copyfile]
        assert pipe.push()==[cfg_copyfile]
        assert pipe.push_preview()==[]
        pipe._cache.clear()
        assert pipe.pull_preview()==[]

        # doesn't take files not meet pattern
        cfg_copyfile2 = 'test.xlsx'
        df.to_csv(pipe.dirpath/cfg_copyfile2)
        assert pipe.push_preview()==[]
        (pipe.dirpath/cfg_copyfile2).unlink()

        # crc works
        df2 = pd.read_csv(pipe.dirpath/cfg_copyfile, **pipe.readparams)
        df2.to_csv(pipe.dirpath/cfg_copyfile, index=False)
        assert pipe.push_preview()==[]
        df.to_csv(pipe.dirpath/cfg_copyfile, index=True)
        assert pipe.push_preview()==[cfg_copyfile]

        # files param works
        assert pipe.pull(files=[cfg_copyfile])==[cfg_copyfile]
        pipe._pullpush_luigi([cfg_copyfile], 'remove')
        assert pipe._pullpush_luigi([cfg_copyfile], 'exists') == [False]
        assert pipe.push(files=[cfg_copyfile])==[cfg_copyfile]
        assert pipe._pullpush_luigi([cfg_copyfile], 'exists') == [True]

        # remove_orphans works
        (pipe.dirpath/cfg_copyfile).unlink()
        pipe._cache.clear()
        assert pipe.remove_orphans('both',dryrun=True)['remote']==[cfg_copyfile]
        assert pipe.remove_orphans('both',dryrun=False)['remote']==[cfg_copyfile]
        assert pipe._pullpush_luigi(['test.csv'],'exists')==[False]

        # cleanup
        pipe._empty_local(confirm=False,delete_all=True)

        # def test_pipes_includeexclude(self, cleanup, remoteinit, pipeinit, testcfg):
    #     pass
        '''
        # check include/exclude
        cfg_name = 'test'
        data = {
            'name': 'vendorX-tutorial',
            'remote': 'vendorX-tutorial',
            'settings': {
                'remotedir': 'vendorX/',
                'include': 'machinedata-2018-01*.csv',
            }
        }
    
        _ = api.cnxn.pipes._('vendorX-tutorial').patch(request_body=data)
        r, d = api.cnxn.pipes._(cfg_pipe).filenames.get()
        assert d == cfg_filenames_chk[:1]
    
        data = {
            'name': 'vendorX-tutorial',
            'remote': 'vendorX-tutorial',
            'settings': {
                'remotedir': 'vendorX/',
                'exclude': 'machinedata-2018-01*.csv',
            }
        }
    
        _ = api.cnxn.pipes._('vendorX-tutorial').patch(request_body=data)
        r, d = api.cnxn.pipes._(cfg_pipe).filenames.get()
        assert d == cfg_filenames_chk[1:]
        r, d = api.cnxn.pipes._(cfg_pipe).delete()
        assert r.status_code==204
        r, d = api.cnxn.pipes._(cfg_pipe).get()
        assert d==[]
        '''

    def test_d6tfree(self, cleanup, signup, testcfg):
        if not testcfg.get('local',False):
            # assert False
            cfg_name = 'utest-d6tfree'
            api = getapi(testcfg.get('local', False))

            # test quick create
            d6tpipe.api.create_pipe_with_remote(api, {'name': cfg_name, 'protocol': 'd6tfree'})
            r, d = api.cnxn.pipes._(cfg_name).get()
            assert d['remote']==cfg_name
            r, d = api.cnxn.remotes._(cfg_name).get()
            assert cfg_usr in d['location'] and cfg_name in d['location'] and d['protocol']=='s3'
            assert "aws_session_token" in d['readCredentials'] and "aws_session_token" in d['writeCredentials']
            assert d['readCredentials']['aws_access_key_id']!=d['writeCredentials']['aws_access_key_id']

            # test push/pull
            pipe = getpipe(api, name=cfg_name, mode='all')
            cfg_copyfile = 'test.csv'
            cfg_copyfile2 = 'test2.csv'
            pipe._pullpush_luigi([cfg_copyfile,cfg_copyfile2],op='remove')

            df = pd.DataFrame({'a':range(10)})
            df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
            # assert False
            assert pipe.push()==[cfg_copyfile]
            pipe._cache.clear()
            assert pipe.pull()==[cfg_copyfile]

            # permissions - no access
            api2 = getapi2(testcfg.get('local', False))
            with pytest.raises(APIError, match='403'):
                pipe2 = getpipe(api2, name=cfg_name, mode='all')
                pipe2.pull()

            # permissions - read
            settings = {"user": cfg_usr2, "role": "read"}
            d6tpipe.create_or_update_permissions(api, cfg_name, settings)

            pipe2 = getpipe(api2, name=cfg_name, mode='all')
            assert pipe2.pull()==[cfg_copyfile]

            df.to_csv(pipe2.dirpath / cfg_copyfile2, index=False)
            with pytest.raises(ValueError, match='No write credentials'):
                pipe2.push()

            # permissions - write
            settings = {"user": cfg_usr2, "role": "write"}
            d6tpipe.create_or_update_permissions(api, cfg_name, settings)

            pipe2 = getpipe(api2, name=cfg_name, mode='all', chk_empty=False)
            assert pipe2.pull()==[cfg_copyfile]
            assert pipe2.push()==[cfg_copyfile,cfg_copyfile2]

            # cleanup
            pipe._pullpush_luigi([cfg_copyfile,cfg_copyfile2],op='remove')
            assert pipe.scan_remote(cached=False)[1]==[]
            pipe._empty_local(confirm=False,delete_all=True)


    def test_sftp(self, cleanup, signup, testcfg):
        cfg_name = cfg_settings_remote_sftp['name']
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.api.create_pipe_with_remote(api, cfg_settings_remote_sftp, cfg_settings_pipe_sftp)

        # test push/pull
        pipe = getpipe(api, name=cfg_name, mode='all')
        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert pipe.scan_remote(cached=False)[1]==[]
        assert pipe.pull()==[]
        # assert False
        assert pipe.push_preview()==[cfg_copyfile]
        assert pipe.push()==[cfg_copyfile]
        pipe._cache.clear()
        assert pipe.pull()==[cfg_copyfile]

        # cleanup
        pipe._pullpush_luigi([cfg_copyfile],op='remove')
        assert pipe.scan_remote(cached=False)[1]==[]
        pipe._empty_local(confirm=False,delete_all=True)



    def test_ftp(self, cleanup, signup, testcfg):
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.api.create_or_update_from_json(api.cnxn.remotes, 'tests/.creds-test.json', 'remotes-test-ftp')
        d6tpipe.api.create_or_update_from_json(api.cnxn.pipes, 'tests/.creds-test.json', 'pipes-test-ftp')
        cfg_name = 'test-ftp'

        # test push/pull
        pipe = getpipe(api, name=cfg_name, mode='all')
        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert pipe.scan_remote(cached=False)[1]==[]
        assert pipe.pull()==[]
        # assert False
        assert pipe.push_preview()==[cfg_copyfile]
        assert pipe.push()==[cfg_copyfile]
        pipe._cache.clear()
        assert pipe.pull()==[cfg_copyfile]
        pipe._pullpush_luigi(['test.csv'],op='remove')
        assert pipe.scan_remote(cached=False)[1]==[]
        pipe._empty_local(confirm=False,delete_all=True)



class TestAdvanced(object):
    scenarios = [scenario0]

    @pytest.fixture(scope="class")
    def init(self):
        api = getapi(local=True)
        assert api.cnxn.remotes.get()[1]==[]
        assert api.cnxn.pipes.get()[1]==[]
        api.cnxn.remotes.post(request_body=cfg_settings_remote)
        api.cnxn.pipes.post(request_body=cfg_settings_pipe)
        yield True
        api.cnxn.remotes._(cfg_remote).delete()
        api.cnxn.pipes._(cfg_pipe).delete()

    @pytest.fixture()
    def pull(self):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe)
        pipe.pull()
        yield True
        pipe._empty_local(confirm=False,delete_all=True)

    def test_includeexclude(self, cleanup, init, pull):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe)
        assert pipe.scan_local(include='machinedata-2018-01*.csv', names_only=True)[1] == cfg_filenames_chk[:1]
        assert pipe.scan_local(exclude='machinedata-2018-01*.csv', names_only=True)[1] == cfg_filenames_chk[1:]
        assert pipe.files(include='machinedata-2018-01*.csv') == [pipe.dirpath/s for s in cfg_filenames_chk[:1]]
        assert pipe.files(exclude='machinedata-2018-01*.csv') == [pipe.dirpath/s for s in cfg_filenames_chk[1:]]

    def test_pipes_import(self, cleanup, init):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe)
        cfg_copyfile = 'test2.csv'
        pathout = Path(cfg_copyfile)
        df = pd.DataFrame({'a': range(10)})
        df.to_csv(cfg_copyfile, index=False)

        pipe.import_file(cfg_copyfile)
        pathin = pipe.dirpath / cfg_copyfile
        assert pathin.exists()
        assert pathout.exists()

        pipe.import_file(cfg_copyfile, move=True)
        assert pathin.exists()
        assert not pathout.exists()

        pathin.unlink()


class TestEncrypt(object):
    scenarios = [scenario0]

    def test(self):
        config = {'filerepo': '~/d6tpipe/files/default/','filereporoot': '~/d6tpipe/',
     'filedb': '~/d6tpipe/files/default/db.json',
     'encrypt': True, 'key': 'ccfc3bf8-a990-4030-a775-a8dc6738dd4f'}

        api = d6tpipe.api.APILocal(config=config)
        var = api.encode(cfg_settings_remote)
        assert 'name' in var
        assert api.decode(var)['readCredentials'] == cfg_settings_remote['readCredentials']

