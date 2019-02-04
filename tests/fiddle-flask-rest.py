from flask import Flask
from flask import request, jsonify
from flask_restplus import Api, Resource, fields
from werkzeug.contrib.fixers import ProxyFix

import logging
from tinydb import TinyDB, Query

from .utils import TinydbREST
cfg_db = 'db.json'

app = Flask(__name__)
app.url_map.strict_slashes = False
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(app, version='1.0', title='Databolt PIPE API',
    description='Share and exchange data',
)

nsr = api.namespace('remotes', description='REMOTE operations')
nsp = api.namespace('pipes', description='PIPE operations')

model_remotes = api.model('remotes', {
    'name': fields.String(readonly=True, description='unique name'),
    'protocol': fields.String(required=True, description='remote protocol'),
    'location': fields.String(required=True, description='remote location'),
})

model_pipes = api.model('pipes', {
    'name': fields.String(readonly=True, description='unique name'),
    'remote': fields.String(required=True, description='remote name'),
})


@app.route('/api')
def rte_root():
    return jsonify({"description":"d6tpipe API","version":1.0})


@app.route('/wipeall', methods=['POST'])
def rte_wipeall():
    tdb = TinyDB(cfg_db)
    tdb.purge_tables()
    return jsonify({'status':'wiped','db':tdb.all()})


@nsr.route('/')
class RemoteList(Resource):
    '''Shows a list of all items, and lets you POST to add new ones'''
    @nsr.doc('list_remotes')
    # @nsr.marshal_list_with(model_remotes)
    def get(self):
        '''List all'''
        return TinydbREST(cfg_db, 'remotes').get()[1]

    @nsr.doc('create_remote')
    @nsr.expect(model_remotes)
    @nsr.marshal_with(model_remotes, code=201)
    def post(self):
        '''Create new'''
        app.logger.info('put'+str(api.payload))
        return TinydbREST(cfg_db, 'remotes').post(request_body=api.payload)[1], 201


@nsr.route('/<string:name>')
@nsr.response(404, 'Not found')
@nsr.param('name', 'The identifier')
class RemoteDetail(Resource):
    '''Show a single item and lets you delete them'''
    @nsr.doc('get_remote')
    @nsr.marshal_with(model_remotes)
    def get(self, name):
        '''Fetch a given resource'''
        return TinydbREST(cfg_db, 'remotes', name).get()[1]

    @nsr.doc('delete_remote')
    @nsr.response(204, 'Todo deleted')
    def delete(self, name):
        '''Delete an item given its name'''
        TinydbREST(cfg_db, 'remotes', name).delete()[1]
        return '', 204

    @nsr.expect(model_remotes)
    @nsr.marshal_with(model_remotes)
    def put(self, name):
        '''Update an item given its name'''
        return TinydbREST(cfg_db, 'remotes', name).put(request_body=api.payload)[1]

    @nsr.expect(model_remotes)
    @nsr.marshal_with(model_remotes)
    def patch(self, name):
        '''Update an item given its name'''
        return TinydbREST(cfg_db, 'remotes', name).put(request_body=api.payload)[1]
