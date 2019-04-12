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
        d6tpipe.list_profiles()

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
        settings = copy.deepcopy(cfg_settings_parent)
        if testcfg.get('encrypt',True):
            settings = api.encode(settings)

        # check clean
        r, d = api.cnxn.pipes.get()
        assert d==[]
        assert cfg_parent_name not in api.list_pipes()

        # create
        if testcfg.get('local',False):
            settings['protocol']='s3'
            settings['options']={'remotepath': 's3://{}/'.format(cfg_settings_parent['location'])}
        response, data = d6tpipe.upsert_pipe(api, settings)
        assert cfg_parent_name in api.list_pipes()
        assert response.status_code == 201
        yield True

        r, d = api.cnxn.pipes._(cfg_parent_name).delete()
        assert r.status_code==204
        # r, d = api.cnxn.pipes.get()
        # assert d==[]



    def test_apiparent(self, cleanup, signup, parentinit, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = copy.deepcopy(cfg_settings_parent)

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

        if not testcfg.get('local',False):
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
        response, data = d6tpipe.upsert_pipe(api, settings2)
        r, d = api.cnxn.pipes.get()
        assert len(d)==2
        settings2["options"] = {'dir':'dir2'}
        response, data = d6tpipe.upsert_pipe(api, settings2)
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
        settings = copy.deepcopy(cfg_settings_pipe)
        assert cfg_pipe_name not in api.list_pipes()

        if testcfg.get('local',False):
            settings['protocol']='s3'
            settings['credentials']=cfg_settings_parent['credentials']
            settings['options']={**settings['options'],**{'remotepath': 's3://{}/{}'.format(cfg_settings_parent['location'],cfg_settings_pipe['options']['dir'])}}
        response, data = d6tpipe.upsert_pipe(api, settings)
        assert cfg_pipe_name in api.list_pipes()
        yield True

        r, d = api.cnxn.pipes._(cfg_pipe_name).delete()
        assert r.status_code == 204

    def test_apipipes(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        settings = copy.deepcopy(cfg_settings_pipe)

        r, d = api.cnxn.pipes._(cfg_pipe_name).get()
        assert d['options']['remotepath'] == 's3://{}/{}/'.format(cfg_settings_parent['location'],cfg_settings_pipe['options']['dir'].strip('/'))

        if not testcfg.get('local',False):
            assert d['parent-ultimate'] == cfg_parent_name
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

            def helper(_api, npipes, pipename, iswrite, isadmin):
                rp = _api.cnxn.pipes.get()[1]
                assert len(rp)==npipes
                assert len(_api.list_pipes()) == npipes
                if npipes==2:
                    l = _api.list_pipes()
                    assert cfg_parent_name in l and cfg_pipe_name in l
                if npipes==0:
                    with pytest.raises(APIError, match='403'):
                        _api.cnxn.pipes._(pipename).get()
                else:
                    rpd = _api.cnxn.pipes._(pipename).get()[1]
                    if iswrite:
                        assert rpd['role']=='write'
                        r, d = _api.cnxn.pipes._(pipename).credentials.get(query_params={'role': 'write'})
                        assert len(d)>0
                    else:
                        assert rpd['role']=='read'
                        r, d = _api.cnxn.pipes._(pipename).credentials.get(query_params={'role': 'read'})
                        assert len(d)>0
                        with pytest.raises(APIError, match='403'):
                            _api.cnxn.pipes._(pipename).credentials.get(query_params={'role': 'write'})
                    if not isadmin:
                        with pytest.raises(APIError, match='403'):
                            _api.cnxn.pipes._(pipename).put(request_body={'bad': 'request'})

            def helperowner():
                helper(api, 2, cfg_pipe_name, True, True)

            helperowner()

            # private
            def helperperm(npipes,pipename,iswrite,npipes2=0):
                helper(api2, npipes=npipes, pipename=pipename, iswrite=iswrite, isadmin=False)
                # helper(api3, nremotes=nremotes2, npipes=npipes2, iswrite=False, isadmin=False)

            helperperm(0,cfg_parent_name,False)
            d6tpipe.upsert_permissions(api,cfg_parent_name,{"username": cfg_usr2, "role": "read"})
            helperperm(2,cfg_parent_name,False)
            d6tpipe.upsert_permissions(api,cfg_parent_name,{"username": cfg_usr2, "role": "revoke"})
            helperperm(0,cfg_parent_name,False)
            api.cnxn.pipes._(cfg_pipe_name).permissions.post(request_body={"username": cfg_usr2, "role": "read"})
            helperperm(1,cfg_pipe_name,False)
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_parent_name).get()
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_parent_name).credentials.get(query_params={'role':'read'})
            api.cnxn.pipes._(cfg_pipe_name).permissions.post(request_body={"username": cfg_usr2, "role": "revoke"})
            helperperm(0,cfg_pipe_name,False)
            if 0==1: # todo: show error if user has no existing permissions that need revoke?
                api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": cfg_usr2, "role": "revoke"})

            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "write", 'expiration_date':(datetime.datetime.now()-datetime.timedelta(days=2)).date().strftime('%Y-%m-%d')})
            helperperm(0,cfg_parent_name,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "write"})
            helperperm(2,cfg_parent_name,True)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "revoke"})
            helperperm(0,cfg_parent_name,False)
            api.cnxn.pipes._(cfg_pipe_name).permissions.post(request_body={"email": cfg_usr2+'@domain.com', "role": "write"})
            helperperm(1,cfg_pipe_name,True)
            api.cnxn.pipes._(cfg_pipe_name).permissions.post(request_body={"username": cfg_usr2, "role": "revoke"})
            helperperm(0,cfg_pipe_name,False)

            # todo: get access to child, don't have access to parent

            # public
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "read"})
            helperperm(2,cfg_parent_name,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "write"}) # public write disabled for now
            helperperm(0,cfg_parent_name,False)
            api.cnxn.pipes._(cfg_parent_name).permissions.post(request_body={"username": "public", "role": "revoke"})
            helperperm(0,cfg_parent_name,False)


    def test_pipes_pull(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api)
        assert pipe.name in api.list_pipes()

        cfg_chk_crc = ['8a9782e9efa8befa9752045ca506a62e',
         '5fe579d6b71031dad399e8d4ea82820b',
         '4c7da169df85253d7ff737dde1e7400b',
         'ca62a122993494e763fd1676cce95e76']

        # assert False
        assert pipe.files() == []
        assert pipe.scan_remote() == cfg_filenames_chk
        r, d = pipe.scan_remote(attributes=True)
        assert _filenames(d) == cfg_filenames_chk
        assert [o['crc'] for o in d]==cfg_chk_crc

        assert api.list_local_pipes()==[]
        assert pipe.pull_preview() == cfg_filenames_chk
        assert pipe.pull() == cfg_filenames_chk
        assert pipe.pull_preview() == []
        assert api.list_local_pipes()==[pipe.name]

        assert pipe.files() == cfg_filenames_chk
        assert pipe.filepaths() == [Path(pipe.dirpath)/f for f in pipe.files()]
        assert pipe.filepaths(aspathlib=False) == [str(Path(pipe.dirpath)/f) for f in pipe.files()]

        pipe = getpipe(api, chk_empty=False, mode='all')
        assert pipe.pull_preview() == cfg_filenames_chk

        # PipeLocal
        pipelocal = d6tpipe.PipeLocal(pipe.name,profile=cfg_profile, filecfg=cfg_cfgfname)
        assert pipelocal.files() == cfg_filenames_chk
        assert pipelocal.scan_local() == cfg_filenames_chk
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
        pipe.delete_files_local(confirm=False,delete_all=True)

    def test_pipes_push(self, cleanup, signup, parentinit, pipeinit, testcfg):
        api = getapi(testcfg.get('local',False))
        pipe = getpipe(api, chk_empty=False)
        pipe.delete_files_local(confirm=False,delete_all=True)
        assert pipe.scan_local()==[]
        pipe = getpipe(api)
        with pytest.raises(PushError):
            pipe.push_preview()
        pipe.pull()

        # push works
        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert set(pipe.scan_local())==set(cfg_filenames_chk+[cfg_copyfile])
        assert pipe.files()==cfg_filenames_chk
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

        # todo: push exclude

        # files() works
        assert pipe.files()==cfg_filenames_chk+[cfg_copyfile]
        assert pipe.files(include='Machine*.csv')==cfg_filenames_chk
        assert pipe.files(exclude='Machine*.csv')==[cfg_copyfile]
        assert pipe.files(sortby='mod')[-1]==cfg_copyfile

        # crc works
        df2 = pd.read_csv(pipe.dirpath/cfg_copyfile, **pipe.schema['pandas'])
        df2.to_csv(pipe.dirpath/cfg_copyfile, index=False)
        assert pipe.push_preview()==[]
        df.to_csv(pipe.dirpath/cfg_copyfile, index=True)
        assert pipe.push_preview()==[cfg_copyfile]

        # files param works
        assert pipe.pull(files=[cfg_copyfile])==[cfg_copyfile]
        pipe.delete_files_remote(files=[cfg_copyfile], confirm=False)
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
        pipe.delete_files_local(confirm=False,delete_all=True)
        assert pipe.scan_local()==[]

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

    def test_d6tfree_share(self, cleanup, signup, testcfg):
        if not testcfg.get('local',False):

            cfg_name = 'utest-d6tfree'
            cfg_name_child = cfg_name+'-child'
            api = getapi(testcfg.get('local', False))
            api2 = getapi2(testcfg.get('local', False))

            # test quick create
            d6tpipe.upsert_pipe(api, {'name': cfg_name})
            d6tpipe.upsert_pipe(api, {'name': cfg_name_child, 'parent':cfg_name, 'options':{'dir':'child'}})
            lp = api.list_pipes()
            assert cfg_name in lp and cfg_name_child in lp

            # share parent pipe
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_name).get()
            d6tpipe.upsert_permissions(api, cfg_name, {'username':cfg_usr2, 'role':'read'})
            assert api2.cnxn.pipes._(cfg_name).get()[1]['name']==cfg_name
            assert api2.cnxn.pipes._(cfg_name_child).get()[1]['name']==cfg_name_child
            d6tpipe.upsert_permissions(api, cfg_name, {'username':cfg_usr2, 'role':'revoke'})
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_name).get()

            # share child pipe
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_name).get()
            d6tpipe.upsert_permissions(api, cfg_name_child, {'username':cfg_usr2, 'role':'read'})
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_name).get()
            assert api2.cnxn.pipes._(cfg_name_child).get()[1]['name']==cfg_name_child
            d6tpipe.upsert_permissions(api, cfg_name_child, {'username':cfg_usr2, 'role':'revoke'})
            with pytest.raises(APIError, match='403'):
                api2.cnxn.pipes._(cfg_name_child).get()

    def test_d6tfree(self, cleanup, signup, testcfg):
        if not testcfg.get('local',False):

            cfg_name = 'utest-d6tfree'
            api = getapi(testcfg.get('local', False))

            # test quick create
            d6tpipe.upsert_pipe(api, {'name': cfg_name})
            r, d = api.cnxn.pipes._(cfg_name).get()
            assert cfg_usr in d['options']['remotepath'] and cfg_name in d['options']['remotepath'] and d['protocol']=='s3'
            cred_read = api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role':'read'})[1]
            cred_write = api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role': 'write'})[1]
            assert "aws_session_token" in cred_read and "aws_session_token" in cred_write
            assert cred_read['aws_access_key_id']!=cred_write['aws_access_key_id']

            # assert False

            # test force renew
            pipe = getpipe(api, name=cfg_name, mode='all')
            pipe._reset_credentials()
            cred_read2 = api.cnxn.pipes._(cfg_name).credentials.get(query_params={'role':'read'})[1]
            assert cred_read2['aws_access_key_id'] != cred_read['aws_access_key_id']

            # test push/pull
            cfg_copyfile = 'folder/test.csv'
            cfg_copyfile2 = 'folder/test2.csv'
            pipe.delete_files_remote(confirm=False)

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

            # todo: check don't have access to parent paths in s3
            # todo: file include patterns
            # d6tpipe.upsert_pipe(api,{'name':'demo-vendor-daily','parent':'demo-vendor','options':{'include':'*daily*.csv'}})
            # d6tpipe.upsert_pipe(api,{'name':'demo-vendor-monthly','parent':'demo-vendor','options':{'include':'*monthly*.csv'}})

            # cleanup
            pipe.delete_files_remote(confirm=False)
            assert pipe.scan_remote(cached=False)==[]
            pipe.delete_files_local(confirm=False,delete_all=True)

    def test_intro_stat_learning(self, cleanup, signup, testcfg):
        cfg_name = cfg_settings_islr['name']
        cfg_filenames_islr = ['Advertising.csv', 'Advertising2.csv', 'Auto.csv', 'Ch10Ex11.csv', 'College.csv', 'Credit.csv', 'Heart.csv', 'Income1.csv', 'Income2.csv', 'LICENSE.md', 'README.md']

        # start with local repo
        pipelocal = d6tpipe.PipeLocal(cfg_name, profile=cfg_profile, filecfg=cfg_cfgfname)
        pipelocal.delete_files_local(confirm=False,delete_all=False)
        pipelocal.import_dir('tests/intro-stat-learning/')
        assert pipelocal.scan_local() == cfg_filenames_islr
        assert pipelocal.files() == []
        assert pipelocal.files(fromdb=False) == cfg_filenames_islr

        df = pd.read_csv(pipelocal.dirpath/'Advertising.csv')
        assert not df.empty

        if not testcfg.get('local', False):
            # set up public repo
            api = getapi()
            d6tpipe.upsert_pipe(api, cfg_settings_islr)
            d6tpipe.upsert_permissions(api, cfg_name, {"username": 'public', "role": "read"})
            pipe = d6tpipe.Pipe(api,cfg_name,mode='all')
            pipe.delete_files_remote(confirm=False)
            assert pipe.scan_remote(cached=False) == []
            assert pipe.push()==cfg_filenames_islr
            pipelocal = d6tpipe.PipeLocal(cfg_name, profile=cfg_profile, filecfg=cfg_cfgfname)
            assert len(pipelocal.schema)>0

            api2 = getapi2()
            pipe = d6tpipe.Pipe(api2,cfg_name)
            pipe.delete_files_local(confirm=False, delete_all=False)
            assert pipe.pull()==cfg_filenames_islr

            df = pd.read_csv(pipe.dirpath / 'Advertising.csv', **pipe.schema['pandas'])
            assert not df.empty

            import dask.dataframe as dd
            files = pipe.filepaths(include='Advertising*.csv')
            ddf = dd.read_csv(files, **pipe.schema['dask'])
            assert not ddf.compute().empty
            pipe.delete_files_local(confirm=False, delete_all=False)

        pipelocal.delete_files_local(confirm=False,delete_all=True)

    def test_sftp(self, cleanup, signup, testcfg):
        cfg_name = cfg_settings_pipe_sftp['name']
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.upsert_pipe(api, cfg_settings_parent_sftp)
        d6tpipe.upsert_pipe(api, cfg_settings_pipe_sftp)

        # test paths
        r,d = api.cnxn.pipes._(cfg_settings_parent_sftp['name']).get()
        assert d['options']['remotepath']=='/'
        r,d = api.cnxn.pipes._(cfg_settings_pipe_sftp['name']).get()
        assert d['options']['remotepath']==cfg_settings_pipe_sftp['options']['dir']+'/'

        # test push/pull
        pipe = getpipe(api, name=cfg_name, mode='all')
        pipe.delete_files_remote(confirm=False)

        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert pipe.scan_remote(cached=False)==[]
        assert pipe.pull()==[]
        assert pipe.push_preview()==[cfg_copyfile]
        assert pipe.push()==[cfg_copyfile]
        pipe._cache_scan.clear()
        assert pipe.pull()==[cfg_copyfile]

        # cleanup
        pipe.delete_files_remote(confirm=False)
        assert pipe.scan_remote(cached=False)==[]
        pipe.delete_files_local(confirm=False,delete_all=True)


    @pytest.fixture(scope="function")
    def setup_ftp_base(self, testcfg):
        api = getapi(testcfg.get('local', False))
        d6tpipe.upsert_pipe_json(api, 'tests/.creds-test.json', 'pipe-test-ftp')

        settings = d6tpipe.utils.loadjson('tests/.creds-test.json')['pipe-test-ftp']
        settings.pop('options')
        settings['options'] = {'include':'root*.csv'}

        if testcfg.get('local', False):
            settings['options']['remotepath'] = '/'

        d6tpipe.upsert_pipe(api, settings)
        pipe = d6tpipe.Pipe(api, settings['name'])
        yield pipe

        pipe.delete_files_local(confirm=False,delete_all=True)

    def test_ftp_base(self, cleanup, signup, setup_ftp_base, testcfg):
        # assert False
        pipe = setup_ftp_base
        files = pipe.scan_remote()
        assert files[0][0]!='/' # check no root dir
        files2 = pipe.pull()
        assert len(files2)==len(files)
        assert len(pipe.scan_local())==len(files)

    @pytest.fixture(scope="function")
    def setup_sftp_base(self, testcfg):
        api = getapi(testcfg.get('local', False))
        settings = \
        {
            'name':'testftp',
            'protocol':'sftp',
            'location':'test.rebex.net',
            'credentials':{'username':'demo', 'password':'password'},
            'options': {'include':'*.txt'}
        }

        if testcfg.get('local', False):
            settings['options'] = {**{'remotepath': '/'}, **settings.get('options',{})}

        d6tpipe.upsert_pipe(api, settings)
        pipe = d6tpipe.Pipe(api, settings['name'])
        yield pipe

        pipe.delete_files_local(confirm=False,delete_all=True)

    def test_sftp_base(self, cleanup, signup, setup_sftp_base, testcfg):
        # assert False
        pipe = setup_sftp_base
        files = pipe.scan_remote()
        assert files[0][0]!='/' # check no root dir
        assert len(files)==2
        files2 = pipe.pull()
        assert len(files2)==len(files)
        assert len(pipe.scan_local())==len(files)

    def test_ftp(self, cleanup, signup, testcfg):
        api = getapi(testcfg.get('local', False))

        # test quick create
        d6tpipe.upsert_pipe_json(api, 'tests/.creds-test.json', 'pipe-test-ftp')
        cfg_name = 'test-ftp'

        # test paths
        r,d = api.cnxn.pipes._(cfg_name).get()
        assert d['options']['remotepath']=='/utest/'

        # test push/pull
        pipe = getpipe(api, name=cfg_name, mode='all')
        cfg_copyfile = 'test.csv'
        df = pd.DataFrame({'a':range(10)})
        df.to_csv(pipe.dirpath/cfg_copyfile,index=False)
        assert pipe.scan_remote(cached=False)==[]
        assert pipe.pull()==[]
        assert pipe.push_preview()==[cfg_copyfile]
        assert pipe.push()==[cfg_copyfile]
        pipe._cache_scan.clear()
        assert pipe.pull()==[cfg_copyfile]
        pipe.delete_files(confirm=False,all_local=True)
        assert pipe.scan_remote(cached=False)==[]

    @pytest.fixture(scope="class")
    def setup_demo(self, testcfg):
        if not testcfg.get('local',False):
            iprofile = 'demo'
            d6tpipe.api.ConfigManager(profile=iprofile,filecfg=cfg_cfgfname).init({'server':testcfg.get('server',cfg_server)})
            cleaner(getapi(profile=iprofile),iprofile)
            api = getapi(profile=iprofile)
            api.register(iprofile, iprofile + '@domain.com', iprofile+'password')
            yield api
            with fuckit:
                api.cnxn.pipes._(iprofile).delete()
            api._unregister(iprofile)
            cleaner(getapi(profile=iprofile),iprofile)
        else:
            yield True

    def test_demo(self, testcfg, setup_demo):
        if not testcfg.get('local',False):
            iprofile = 'demo'
            api = setup_demo
            with pytest.raises(APIError, match='cannot register pipes'):
                d6tpipe.upsert_pipe(api,{'name':iprofile})

    def test_public(self, testcfg):
        if not testcfg.get('local',False):
            iprofile = 'public'
            d6tpipe.api.ConfigManager(profile=iprofile,filecfg=cfg_cfgfname).init({'server':testcfg.get('server',cfg_server)})
            cleaner(getapi(profile=iprofile),iprofile)
            api = getapi(profile=iprofile)
            with pytest.raises(APIError, match='Username'):
                api.register(iprofile, iprofile+'@domain.com', iprofile)
            cleaner(getapi(profile=iprofile),iprofile)



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
        pipe.delete_files_local(confirm=False,delete_all=True)

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

