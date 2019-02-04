import pytest
import warnings

import boto3
from botocore.client import ClientError
import fuckit
import pandas as pd
from luigi.contrib.s3 import S3Client

from d6tpipe.exceptions import ResourceExistsError
from .conftest_common import *


@fuckit
def _wipe(_session, testobj):
    d6tpipe.utils.s3.delete_bucket(_session, testobj.cfg_bucket, confirm=False)
    d6tpipe.utils.s3.delete_user(_session, testobj.cfg_usrname_read, confirm=False)
    d6tpipe.utils.s3.delete_user(_session, testobj.cfg_usrname_write, confirm=False)
    wipeapi(getapi, local=True)
    api = getapi(local=True)
    api.wipe_all(confirm=False)

class TestS3(object):


    scenarios = [('blank', {})]
    cfg_pipe = 'utest'
    cfg_bucket = 'd6tpipe-'+cfg_pipe
    cfg_bucket_s3 = 's3://{}/'.format(cfg_bucket)
    cfg_usrname_read = cfg_bucket+'-read'
    cfg_usrname_write = cfg_bucket+'-write'

    @pytest.fixture(scope="class")
    def setup(self):

        settings = d6tpipe.utils.loadjson('tests/.creds-test.json')['aws_root']
        session = boto3.session.Session(
            aws_access_key_id=settings['aws_access_key_id'],
            aws_secret_access_key=settings['aws_secret_access_key'],
        )

        _wipe(session, self)

        # d6tpipe
        assert not p_cfgfname.exists()
        d6tpipe.api.ConfigManager(profile=cfg_profile,filecfg=cfg_cfgfname).init()
        api = getapi(local=True)

        yield session, api

        _wipe(session, self)

        # try:
        #     d6tpipe.utils.s3.delete_bucket(session, self.cfg_bucket, confirm=False)
        #     d6tpipe.utils.s3.delete_user(session, self.cfg_usrname_read, confirm=False)
        #     d6tpipe.utils.s3.delete_user(session, self.cfg_usrname_write, confirm=False)
        #     wipeapi(getapi)
        # except Exception as e:
        #     warnings.warn('tearndown exception: '+str(e))


    def test_create_bucket_with_users(self, setup):

        # init and check clean
        session, api = setup
        s3r = session.resource('s3')
        with pytest.raises(ClientError):
            s3r.meta.client.head_bucket(Bucket=self.cfg_bucket)

        assert api.cnxn.remotes.get()[1]==[]
        assert api.cnxn.pipes.get()[1]==[]

        # helpers
        def _connect(_settings):
            return S3Client(**_settings['readCredentials'])

        def _post(_settings):
            d6tpipe.api.create_or_update(api.cnxn.remotes,_settings)
            d6tpipe.api.create_or_update(api.cnxn.pipes,{'name':self.cfg_pipe,'remote':self.cfg_pipe})

        def _run_pipe():
            pipe = d6tpipe.pipe.Pipe(api, self.cfg_pipe)
            pd.DataFrame({'a':range(10)}).to_csv(pipe.dirpath/'test.csv')
            assert pipe.push(['test.csv'])==['test.csv']
            assert pipe.pull(['test.csv'])==['test.csv']

        def _bucketempty(_cnxn):
            return list(_cnxn.listdir(self.cfg_bucket_s3)) == []
        def _bucketnotempty(_cnxn):
            return list(_cnxn.listdir(self.cfg_bucket_s3)) == [self.cfg_bucket_s3+'test.csv']

        # tests
        settings = d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe)
        _post(settings)
        cnxn = _connect(settings)

        s3r.meta.client.head_bucket(Bucket=self.cfg_bucket)
        assert _bucketempty(cnxn)
        _run_pipe()
        assert _bucketnotempty(cnxn)

        # bucket_exists
        with pytest.raises(ResourceExistsError):
            d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, bucket_exists='raise')
        d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, bucket_exists='keep-keep')
        assert _bucketnotempty(cnxn)
        d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, bucket_exists='keep-new')
        assert _bucketnotempty(cnxn)
        d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, bucket_exists='recreate-noconfirm')
        assert _bucketempty(cnxn)

        # user_exists
        with pytest.raises(ResourceExistsError):
            d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, user_exists='raise')
        settings2 = d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, user_exists='keep-keep')
        assert settings2['readCredentials']['aws_access_key_id'] is None
        settings2 = d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, user_exists='keep-new')
        assert settings2['readCredentials']['aws_access_key_id']!=settings['readCredentials']['aws_access_key_id']
        _post(settings2)
        # assert False
        _run_pipe()
        settings2 = d6tpipe.utils.s3.create_bucket_with_users(session, self.cfg_pipe, user_exists='recreate-noconfirm')
        assert settings2['readCredentials']['aws_access_key_id']!=settings['readCredentials']['aws_access_key_id']
        _post(settings2)
        _run_pipe()




