import pathlib
import pytest
import copy

from tinydb import TinyDB, Query

import d6tpipe.utils
from d6tpipe.http_client.exceptions import APIError


class TestClientTiny():

    cfg_path = pathlib.Path('tests/db-test.json')
    cfg_remote_data = {
        'name': 'test-dev',
        'protocol': 's3',
        'location': 'bucket',
        'readCredentials': {
            'aws_access_key_id': 'AAA', 'aws_secret_access_key': 'BBB'
        },
        'writeCredentials': {
            'aws_access_key_id': 'AAA', 'aws_secret_access_key': 'BBB'
        }
    }

    cfg_pipe_data = data = {
        'name': 'test-dev',
        'remote': 'test-dev',
        'settings': {
            'remotedir': 'subdir/',
        }
    }

    def setup_method(self, method):
        if self.cfg_path.exists():
            self.cfg_path.unlink()

    def teardown_method(self, method):
        try:
            self.cfg_path.unlink()
        except:
            warnings.warn('could not delete db')

    def test_all(self):

        c = d6tpipe.utils.ClientTiny(self.cfg_path)
        assert c.remotes.get()[1]==[]
        assert c.pipes.get()[1]==[]

        # create
        r,d = c.remotes.post(request_body=self.cfg_remote_data)
        assert d==self.cfg_remote_data
        r, d = c.remotes.get()
        assert len(d)==1
        assert d[0]==self.cfg_remote_data

        # get
        r, d = c.remotes._(self.cfg_remote_data['name']).get()
        assert d==self.cfg_remote_data

        # with pytest.raises(APIError):
        # todo: return 404 not found error. on GET and _('pipe').post()
        r, d = c.remotes._('no exist').get()
        assert d==[]

        # test insert again raise error
        with pytest.raises(APIError):
            c.remotes.post(request_body=self.cfg_remote_data)

        # put
        d2=copy.deepcopy(self.cfg_remote_data)
        d2['protocol']='changed'
        r, d = c.remotes._(self.cfg_remote_data['name']).put(d2)
        assert d['protocol']=='changed'
        r, d = c.remotes._(self.cfg_remote_data['name']).get()
        assert d['protocol']=='changed'
        # patch
        r, d = c.remotes._(self.cfg_remote_data['name']).patch({'protocol':self.cfg_remote_data['protocol']})
        assert d['protocol']==self.cfg_remote_data['protocol']
        r, d = c.remotes._(self.cfg_remote_data['name']).get()
        assert d['protocol']==self.cfg_remote_data['protocol']

        # create
        assert c.pipes.get()[1]==[]

        # create pipe
        r,d = c.pipes.post(request_body=self.cfg_pipe_data)
        assert d==self.cfg_pipe_data
        r, d = c.pipes.get()
        assert len(d)==1
        assert d[0]==self.cfg_pipe_data
        # check didn't mess up remotes
        r, d = c.remotes.get()
        assert len(d)==1
        assert d[0]==self.cfg_remote_data

        # add files
        c2 = c.remotes._(self.cfg_remote_data['name'])
        r, d = c2.files.get()
        assert d==[]
        r, d = c2.files.post(request_body=['a.csv','b.csv'])
        db = TinyDB(self.cfg_path)
        assert sorted(list(db.tables()))==sorted(['pipes', '_default', 'remotes-files', 'remotes'])
        r, d = c2.files.get()
        assert d==['a.csv','b.csv']

        # delete
        r, d = c.pipes._(self.cfg_pipe_data['name']).delete()
        assert d==True
        r, d = c.pipes.get()
        assert len(d)==0
        r, d = c.remotes.get()
        assert len(d)==1

        r, d = c.remotes._(self.cfg_remote_data['name']).delete()
        assert d==True
        r, d = c.remotes.get()
        assert len(d)==0

from d6tpipe.pipe import _tinydb_last, _files_diff, _files_mod, _apply_fname_filter

class TestHelpers(object):

    def test_files_diff(self):

        _to = [{'filename':'a.csv','crc':1},{'filename':'b.csv','crc':1}]
        _from = [{'filename':'a.csv','crc':2},{'filename':'b.csv','crc':1}]
        assert _files_mod(_from,_to)==['a.csv']
        assert _files_diff(_from,_to,'default',None,None)==['a.csv']

        _to = [{'filename':'a.csv','crc':1},{'filename':'b.csv','crc':1}]
        _from = [{'filename':'a.csv','crc':1}]
        assert _files_mod(_from,_to)==[]

        _to = [{'filename':'a.csv','crc':1}]
        _from = [{'filename':'a.csv','crc':1},{'filename':'b.csv','crc':1}]
        assert _files_mod(_from,_to)==[]

        _to = [{'filename':'a.csv','crc':1}]
        _from = [{'filename':'a.csv','crc':2},{'filename':'b.csv','crc':1}]
        assert _files_diff(_from,_to,'default',None,None)==['a.csv','b.csv']
        assert _files_diff(_from,_to,'new',None,None)==['b.csv']
        assert _files_diff(_from,_to,'modified',None,None)==['a.csv']

    def test_include_exclude(self):
        l = ['a.csv','b.csv','c.xlsx','d.exe']
        assert _apply_fname_filter(l,include='*.csv',exclude=None)==l[:2]
        assert _apply_fname_filter(l,include=None,exclude='*.exe')==l[:-1]
        assert _apply_fname_filter(l,include='*.csv|*.xlsx',exclude=None)==l[:-1]
        assert _apply_fname_filter(l,include=None,exclude='*.exe|*.xlsx')==l[:2]


