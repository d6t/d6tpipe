def _dict_sort(files, sortby='filename'):
    return sorted(files, key=lambda k: k[sortby])

#**********************************
# filehash
#**********************************

import hashlib
def filemd5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(int(1e7)), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def dfchunk(df, chunk_size=1e6):
    chunk_size = int(chunk_size)
    for istart in range(0,df.shape[0],chunk_size):
        yield (df.iloc[istart:istart + chunk_size,])


def dfmd5(df, chunk_size=1e6):
    hash_md5 = hashlib.md5()
    for chunk in dfchunk(df,chunk_size):
        hash_md5.update(str(chunk).encode('utf-8'))
    return hash_md5.hexdigest()


#**********************************
# tinydb <> Client
#**********************************

from tinydb import TinyDB, Query
from d6tpipe.http_client.exceptions import APIError

from tinydb_serialization import SerializationMiddleware
from d6tpipe.tinydb_serializers import DateTimeSerializer
_tdbserialization = SerializationMiddleware()
_tdbserialization.register_serializer(DateTimeSerializer(), 'TinyDate')


class TinydbREST(object):

    class Request(object):
        def __init__(self, status_code):
            self.status_code = status_code

    def __init__(self, path, table, pk=None):
        self.table = table
        self.db = TinyDB(path, storage=_tdbserialization).table(table)
        self.pk = pk

    def _getpk(self):
        r = self.db.get(Query().name==self.pk)
        if r is None:
            # todo: return 404 not found error. on GET and _('pipe').post()
            raise IOError('{} Not found'.format(self.pk))
        elif 'data' in r.keys():
            return r['data']
        else:
            return r

    def get(self):
        if self.pk is not None:
            return self.Request(200), self._getpk()
        else:
            return self.Request(200), self.db.all()

    def post(self, request_body):
        if self.pk is None:
            name = request_body['name']
            if name in ['pipes','remotes']:
                raise APIError('{} is reserved, use another name'.format(name))
            if self.db.contains(Query().name == name):
                raise APIError('entry {} already exists, needs to be unique'.format(name))
            self.db.insert(request_body)
            self.pk = name
            return self.Request(201), self._getpk()
        else:
            name = self.pk
            self.db.upsert({'name': name, 'data': request_body}, Query().name == self.pk)
            return self.Request(200), self._getpk()

    def put(self, request_body):
        if self.pk is not None:
            self.db.upsert(request_body, Query().name == self.pk)
            return self.Request(200), self._getpk()

    def patch(self, request_body):
        if self.pk is not None:
            dict_ = self._getpk()
            dict_.update(request_body)
            return self.put(dict_)

    def delete(self):
        if self.pk is not None:
            self.db.remove(Query().name == self.pk)
            return self.Request(204),not self.db.contains(Query().name == self.pk)


class ClientTiny(object):

    def __init__(self, path, table=None, pk=None):
        self.path = path
        self.table = table
        self.pk = pk

    def __getattr__(self, item, request_body=None):
        if item in ['get','post','put','patch','delete']:
            if self.table is not None and self.pk is not None:
                return TinydbREST(self.path, self.table, self.pk).__getattribute__(item)
            elif self.table is not None:
                return TinydbREST(self.path, self.table).__getattribute__(item)
            else:
                raise ValueError('for actions, need table and/or pk set')
        else:
            item = item if self.table is None else '-'.join([self.table,item])
            return ClientTiny(self.path, item, self.pk)

    def _(self, name):
        return ClientTiny(self.path, self.table, name)


#**********************************
# yaml json loader
#**********************************
import json
def loadjson(path, encoding='utf-8',strict=False):
    if strict:
        with open(path, 'r', encoding=encoding) as f:
            data = json.load(f)
        return data
    else:
        return loadyaml(path,encoding)

import yaml
def loadyaml(path, encoding='utf-8'):
    with open(path, 'r', encoding=encoding) as f:
        data = yaml.load(f)
    return data

#**********************************
# copy/move folder
#**********************************
import os
import shutil
def copytree(src, dst, move=False, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        print(s,d)
        if os.path.isdir(s):
            if move:
                shutil.move(s, d)
            else:
                shutil.copytree(s, d, symlinks, ignore)
        else:
            if move:
                shutil.move(s, d)
            else:
                shutil.copy2(s, d)
