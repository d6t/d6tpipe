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
import datetime

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

cfg_server = 'https://d6tpipe-staging-demo.herokuapp.com'
cfg_server = 'http://192.168.33.10:5000'

scenario0 = ('blank', {})
scenario1 = ('local', {'testcfg':{'local': True, 'encrypt':False}})
scenario2 = ('remote', {'testcfg':{'encrypt':False}})
scenario3 = ('remote-encrypt', {'testcfg':{}})
scenario4 = ('heroku-staging', {'testcfg':{'server':'https://d6tpipe-staging-demo.herokuapp.com','encrypt':False}})
scenario5 = ('heroku-prod', {'testcfg':{'server':'https://pipe.databolt.tech','encrypt':False}})
# scenario5 = ('heroku-prod', {'testcfg':{'server':'https://d6tpipe-prod-live.herokuapp.com'}})

# ************************************
# setup
# ************************************
class TestMain(object):
    scenarios = [scenario2]
    # scenarios = [scenario1, scenario2, scenario3]
    # scenarios = [scenario1]#[scenario1, scenario2, scenario3]
    # scenarios = [scenario4, scenario5]

    @pytest.fixture(scope="class")
    def cleanup(self, testcfg):
        # make sure nothing exists before starting tests

        cleaner(getapi,cfg_usr)
        cleaner(getapi2,cfg_usr2)

        assert not os.path.exists(cfg_cfgfname2)

        d6tcollect.host = testcfg.get('server',cfg_server)

        # cfg init
        [d6tpipe.api.ConfigManager(profile=iprofile,filecfg=cfg_cfgfname).init({'server':testcfg.get('server',cfg_server)}) for iprofile in [cfg_profile,cfg_profile2,cfg_profile3]]

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
    def parentinit(self, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_parent
        if testcfg.get('encrypt',True):
            settings = api.encode(settings)

        # check clean
        r, d = api.cnxn.pipes.get()
        assert d==[]
        assert cfg_parent_name not in api.list_pipes()

        # create
        response, data = d6tpipe.api.upsert_pipe(api, settings)
        assert cfg_parent_name in api.list_pipes()
        assert response.status_code == 201
        yield True

        r, d = api.cnxn.pipes._(cfg_parent_name).delete()
        assert r.status_code==204
        # r, d = api.cnxn.pipes.get()
        # assert d==[]



    def test_apiparent(self, cleanup, signup, parentinit, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_parent

        # no duplicate names
        with pytest.raises(APIError, match='unique'):
            response, data = api.cnxn.pipes.post(request_body=settings)
        # todo: another user creates

        if not testcfg.get('local',False):
            # only alphanumeric
            with pytest.raises(APIError, match='alphanumeric'):
                settingsbad = settings.copy()
                settingsbad['name']='$invalid'
                api.cnxn.pipes.post(request_body=settingsbad)
            with pytest.raises(APIError, match='Paths'):
                settingsbad = settings.copy()
                settingsbad['name']='invalid'
                settingsbad['options']={'dir':'C:\\Drive'}
                api.cnxn.pipes.post(request_body=settingsbad)

        r, d = api.cnxn.pipes._(cfg_parent_name).get()
        if testcfg.get('encrypt',True):
            # check can't decode with wrong key
            k = api.key; api.key = 'wrong';
            with pytest.raises(InvalidSignatureError):
                api.decode(d)
            api.key = k

            d = api.decode(d)

        # check values
        assert d['name'] == cfg_settings_parent['name']
        for ikey in ['isPubliclyReadable','isPubliclyWritable','credentials','cache']:
            assert ikey not in d
        assert d['role'] == 'write'
        assert d['options']['remotepath'] == 's3://{}/'.format(cfg_settings_parent['location'])

        # create and delete
        r, d = api.cnxn.pipes.get()
        assert len(d)==1
        settings2 = settings.copy()
        settings2['name'] = cfg_parent_name+'2'
        settings2["options"] = {'dir':'dir1'}
        response, data = d6tpipe.api.upsert_pipe(api, settings2)
        r, d = api.cnxn.pipes.get()
        assert len(d)==2
        settings2["options"] = {'dir':'dir2'}
        response, data = d6tpipe.api.upsert_pipe(api, settings2)
        assert api.cnxn.pipes._(settings2['name']).get()[1]["options"]['dir'] == "dir2"

        r, d = api.cnxn.pipes._(settings2['name']).delete()
        assert r.status_code == 204
        r, d = api.cnxn.pipes.get()
        assert len(d)==1

        # permissions
        if not testcfg.get('local',False):
            api2 = getapi2()
            with pytest.raises(APIError, match='403'):
                r, d = api2.cnxn.pipes._(cfg_parent_name).get()


    @pytest.fixture()
    def pipeinit(self, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_pipe
        assert cfg_pipe_name not in api.list_pipes()

        # response, data = d6tpipe.api.upsert_pipe(api, settings)
        settings['name']=settings['name']
        api.cnxn.pipes.post(request_body=settings)
        assert cfg_pipe_name in api.list_pipes()
        yield True

        r, d = api.cnxn.pipes._(cfg_pipe_name).delete()
        assert r.status_code == 204

    def test_apipipes(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = cfg_settings_pipe

        r, d = api.cnxn.pipes._(cfg_pipe_name).get()
        assert d['options']['remotepath'] == 's3://{}/{}/'.format(cfg_settings_parent['location'],cfg_settings_pipe['options']['dir'].strip('/'))
        assert d['parent-ultimate']==cfg_parent_name

        if not testcfg.get('local',False):
            # non-existing parent
            with pytest.raises(APIError, match='does not exist'):
                settingsbad = settings.copy()
                settingsbad['parent']='noexist'
                settingsbad['name']='invalid'
                api.cnxn.pipes.post(request_body=settingsbad)

        # todo: delete parent, child pipes deleted

        # check permissions
        if not testcfg.get('local',False):
            api2 = getapi2()

            r, d = api2.cnxn.pipes.get()
            assert d == []

            with pytest.raises(APIError, match='403'): # todo: 404 vs 403 error?
                r, d = api2.cnxn.pipes._(cfg_pipe_name).get()

    def test_permissions(self, cleanup, signup, parentinit, pipeinit, testcfg):
        if not testcfg.get('local',False):
            api = getapi(testcfg.get('local',False))
            api2 = getapi2(testcfg.get('local',False))
            api3 = getapi(testcfg.get('local',False),cfg_profile3)

            def helper(_api, npipes, iswrite, isadmin):
                rp = _api.cnxn.pipes.get()[1]
                assert len(rp)==npipes
                if npipes==0:
                    with pytest.raises(APIError, match='403'):
                        _api.cnxn.pipes._(cfg_pipe_name).get()
                else:
                    rpd = _api.cnxn.pipes._(cfg_pipe_name).get()[1]
                    if iswrite:
                        assert rpd['role']=='write'
                    else:
                        assert rpd['role']=='read'
                    if not isadmin:
                        with pytest.raises(APIError, match='403'):
                            _api.cnxn.pipes._(cfg_pipe_name).put(request_body={'bad': 'request'})

            def helperowner():
                helper(api, npipes=2, iswrite=True, isadmin=True)

            helperowner()

            # private
            def helperperm(npipes,iswrite,npipes2=0):
                helper(api2, npipes=npipes, iswrite=iswrite, isadmin=False)
                # helper(api3, nremotes=nremotes2, npipes=npipes2, iswrite=False, isadmin=False)

            helperperm(0,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": cfg_usr2, "role": "read"})
            helperperm(2,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": cfg_usr2, "role": "revoke"})
            helperperm(0,False)
            api.cnxn.pipes._(cfg_pipe_name).permissions.post(request_body={"username": cfg_usr2, "role": "read"})
            helperperm(1,False)

            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "write", 'expiration_date':(datetime.datetime.now()-datetime.timedelta(days=2)).date().strftime('%Y-%m-%d')})
            helperperm(0,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "write"})
            helperperm(1,True)

            # todo: get access to child, don't have access to parent

            # public
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "read"})
            helperperm(2,False,1,1)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "write"}) # public write disabled for now
            helperperm(0,0,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "revoke"})
            helperperm(0,0,False)


    def test_pipes_pull(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api)
        assert pipe.name in api.list_pipes()

        cfg_chk_crc = ['8a9782e9efa8befa9752045ca506a62e',
         '5fe579d6b71031dad399e8d4ea82820b',
         '4c7da169df85253d7ff737dde1e7400b',
         'ca62a122993494e763fd1676cce95e76']

        r, d = pipe.scan_remote()
        assert _filenames(d) == cfg_filenames_chk
        assert [o['crc'] for o in d]==cfg_chk_crc

        assert api.list_local_pipes()==[]
        assert pipe.pull_preview() == cfg_filenames_chk
        assert pipe.pull() == cfg_filenames_chk
        assert pipe.pull_preview() == []
        assert api.list_local_pipes()==[pipe.name]

        assert pipe.filenames() == cfg_filenames_chk
        assert pipe.files() == [Path(pipe.dirpath)/f for f in pipe.filenames()]
        assert pipe.files(aspathlib=False) == [str(Path(pipe.dirpath)/f) for f in pipe.filenames()]

        pipe = getpipe(api, chk_empty=False, mode='all')
        assert pipe.pull_preview() == cfg_filenames_chk

        # PipeLocal
        pipelocal = d6tpipe.PipeLocal(pipe.name,profile=cfg_profile, filecfg=cfg_cfgfname)
        assert pipelocal.filenames() == cfg_filenames_chk
        assert pipelocal.scan_local_filenames() == cfg_filenames_chk
        assert pipelocal.schema == cfg_settings_pipe['schema']
        df = pd.read_csv(pipe.dirpath/cfg_filenames_chk[0], **pipe.schema['pandas'])

        # permissions
        if not testcfg.get('local',False):
            api2 = getapi2(testcfg.get('local', False))
            with pytest.raises(APIError, match='403'):
                pipe2 = getpipe(api2, name=cfg_pipe_name, mode='all')
                pipe2.pull()

            settings = {"username": cfg_usr2, "role": "read"}
            r,d = d6tpipe.upsert_permissions(api, cfg_parent_name, settings)

            pipe2 = getpipe(api2, name=cfg_pipe_name, mode='all')
            assert pipe2.pull()==cfg_filenames_chk

        # cleanup
        pipe._empty_local(confirm=False,delete_all=True)

    def test_pipes_push(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api, chk_empty=False)
        pipe._empty_local(confirm=False, delete_all=True)
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
        pipe._cache_scan.clear()
        assert pipe.pull_preview()==[]

        # doesn't take files not meet pattern
        cfg_copyfile2 = 'test.xlsx'
        df.to_csv(pipe.dirpath/cfg_copyfile2)
        assert pipe.push_preview()==[]
        (pipe.dirpath/cfg_copyfile2).unlink()

        # crc works
        df2 = pd.read_csv(pipe.dirpath/cfg_copyfile, **pipe.schema['pandas'])
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
        pipe._cache_scan.clear()
        assert pipe.remove_orphans(direction='both',dryrun=True)['remote']==[cfg_copyfile]
        assert pipe.remove_orphans(direction='both',dryrun=False)['remote']==[cfg_copyfile]
        assert pipe._pullpush_luigi(['test.csv'],'exists')==[False]

        # cleanup
        pipe._empty_local(confirm=False,delete_all=True)

        # def test_pipes_includeexclude(self, cleanup, parentinit, pipeinit, testcfg):
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
        r, d = api.cnxn.pipes._(cfg_pipe_name).filenames.get()
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
        r, d = api.cnxn.pipes._(cfg_pipe_name).filenames.get()
        assert d == cfg_filenames_chk[1:]
        r, d = api.cnxn.pipes._(cfg_pipe_name).delete()
        assert r.status_code==204
        r, d = api.cnxn.pipes._(cfg_pipe_name).get()
        assert d==[]
        '''

    def test_d6tfree(self, cleanup, signup, testcfg):
        if not testcfg.get('local',False):
            # assert False
            cfg_name = 'utest-d6tfree'
            api = getapi(testcfg.get('local', False))

            # test quick create
            d6tpipe.api.upsert_pipe(api, {'name': cfg_name, 'protocol': 'd6tfree'})
            r, d = api.cnxn.pipes._(cfg_name).get()
            assert cfg_usr in d['options']['remotepath'] and cfg_name in d['options']['remotepath'] and d['protocol']=='s3'
            cred_read = api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role':'read'})[1]
            cred_write = api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role': 'write'})[1]
            assert "aws_session_token" in cred_read and "aws_session_token" in cred_write
            assert cred_read['aws_access_key_id']!=cred_write['aws_access_key_id']

            # test push/pull
            pipe = getpipe(api, name=cfg_name, mode='all')
            cfg_copyfile = 'folder/test.csv'
            cfg_copyfile2 = 'folder/test2.csv'
            pipe._pullpush_luigi([cfg_copyfile,cfg_copyfile2],op='remove')

            df = pd.DataFrame({'a':range(10)})
            (pipe.dirpath/cfg_copyfile).parent.mkdir(exist_ok=True)
            df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
            # assert False
            assert pipe.push()==[cfg_copyfile]
            pipe._cache_scan.clear()
            assert pipe.pull()==[cfg_copyfile]

            # permissions - no access
            api2 = getapi2(testcfg.get('local', False))
            with pytest.raises(APIError, match='403'):
                pipe2 = getpipe(api2, name=cfg_name, mode='all')
                pipe2.pull()

            # permissions - read
            settings = {"username": cfg_usr2, "role": "read"}
            d6tpipe.upsert_permissions(api, cfg_name, settings)

            pipe2 = getpipe(api2, name=cfg_name, mode='all')
            assert pipe2.role=='read'
            assert pipe2.pull()==[cfg_copyfile]

            df.to_csv(pipe2.dirpath / cfg_copyfile2, index=False)
            with pytest.raises(ValueError, match='Read-only'):
                pipe2.push()

            # permissions - write
            settings = {"username": cfg_usr2, "role": "write"}
            d6tpipe.upsert_permissions(api, cfg_name, settings)

            pipe2 = getpipe(api2, name=cfg_name, mode='all', chk_empty=False)
            assert pipe2.role=='write'
            assert pipe2.pull()==[cfg_copyfile]
            assert pipe2.push()==[cfg_copyfile,cfg_copyfile2]

            # cleanup
            pipe._pullpush_luigi([cfg_copyfile,cfg_copyfile2],op='remove')
            assert pipe.scan_remote(cached=False)[1]==[]
            pipe._empty_local(confirm=False,delete_all=True)


    def test_sftp(self, cleanup, signup, testcfg):
        cfg_name = cfg_settings_parent_sftp['name']
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.api.upsert_pipe(api, cfg_settings_parent_sftp)
        d6tpipe.api.upsert_pipe(api, cfg_settings_pipe_sftp)

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
        pipe._cache_scan.clear()
        assert pipe.pull()==[cfg_copyfile]

        # cleanup
        pipe._pullpush_luigi([cfg_copyfile],op='remove')
        assert pipe.scan_remote(cached=False)[1]==[]
        pipe._empty_local(confirm=False,delete_all=True)



    def test_ftp(self, cleanup, signup, testcfg):
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.api.upsert_pipe_json(api, 'tests/.creds-test.json', 'pipe-test-ftp')
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
        pipe._cache_scan.clear()
        assert pipe.pull()==[cfg_copyfile]
        pipe._pullpush_luigi(['test.csv'],op='remove')
        assert pipe.scan_remote(cached=False)[1]==[]
        pipe._empty_local(confirm=False,delete_all=True)



class TestAdvanced(object):
    scenarios = [scenario0]

    @pytest.fixture(scope="class")
    def init(self):
        api = getapi(local=True)
        assert api.cnxn.pipes.get()[1]==[]
        assert api.cnxn.pipes.get()[1]==[]
        api.cnxn.pipes.post(request_body=cfg_settings_parent)
        api.cnxn.pipes.post(request_body=cfg_settings_pipe)
        yield True
        api.cnxn.pipes._(cfg_parent_name).delete()
        api.cnxn.pipes._(cfg_pipe_name).delete()

    @pytest.fixture()
    def pull(self):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe_name)
        pipe.pull()
        yield True
        pipe._empty_local(confirm=False,delete_all=True)

    def test_includeexclude(self, cleanup, init, pull):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe_name)
        assert pipe.scan_local(include='machinedata-2018-01*.csv', names_only=True)[1] == cfg_filenames_chk[:1]
        assert pipe.scan_local(exclude='machinedata-2018-01*.csv', names_only=True)[1] == cfg_filenames_chk[1:]
        assert pipe.files(include='machinedata-2018-01*.csv') == [pipe.dirpath/s for s in cfg_filenames_chk[:1]]
        assert pipe.files(exclude='machinedata-2018-01*.csv') == [pipe.dirpath/s for s in cfg_filenames_chk[1:]]

    def test_pipes_import(self, cleanup, init):
        api = getapi(local=True)
        pipe = d6tpipe.pipe.Pipe(api, cfg_pipe_name)
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
        var = api.encode(cfg_settings_parent)
        assert 'name' in var
        assert api.decode(var)['readCredentials'] == cfg_settings_parent['readCredentials']

