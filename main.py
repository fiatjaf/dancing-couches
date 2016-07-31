import json
from flask import Flask, request, jsonify, g
from flask_cors import CORS

import settings
import couch
from application import Application

app = Flask(__name__)
app.config.from_object(settings)
CORS(app, supports_credentials=True)


@app.route('/')
def index():
    return 'hello'


@app.route('/<appid>/')
def couch_server_info(appid):
    return jsonify({
        "couchdb": "Welcome",
        "version": "0",
        "vendor": {
            "name": "musical-couches",
            "version": "1",
            "variant":  "crazy"
        }
    })


@app.route('/<appid>/<dbname>/')
def db_info(appid, dbname):
    g.app = Application(appid, dbname)
    return jsonify(couch.info())


@app.route('/<appid>/<dbname>/_local/<path:localid>', methods=['GET', 'PUT'])
def db_local(appid, dbname, localid):
    g.app = Application(appid, dbname)
    if request.method == 'GET':
        return jsonify(couch.local_get(localid))
    else:
        doc = request.get_json()
        ok, rev = couch.local_put(localid, doc)
        return jsonify({
            'ok': ok,
            'rev': rev,
            'id': id
        })


@app.route('/<appid>/<dbname>/_changes', methods=['GET', 'POST'])
def db_changes(appid, dbname):
    g.app = Application(appid, dbname)
    since = int(request.args.get('since') or 0)
    limit = int(request.args.get('limit') or 100)
    return jsonify(couch.changes(since, limit))


@app.route('/<appid>/<dbname>/_revs_diff', methods=['POST'])
def db_revsdiff(appid, dbname):
    g.app = Application(appid, dbname)
    docrevs = request.get_json()
    return jsonify(couch.revsdiff(docrevs))


@app.route('/<appid>/<dbname>/_all_docs')
def db_alldocs(appid, dbname):
    g.app = Application(appid, dbname)
    ids = json.loads(request.args.get('keys') or '[]')

    local = []
    for i, id in enumerate(ids):
        if id.startswith('_local/'):
            couch.local_get(id)
            local.append(i)
    for i in local:
        ids.pop(i)

    return jsonify(couch.alldocs(ids))


@app.route('/<appid>/<dbname>/_bulk_get', methods=['POST'])
def db_bulkget(appid, dbname):
    g.app = Application(appid, dbname)
    docrevs = request.get_json()['docs']
    return jsonify(couch.bulkget(docrevs))


@app.route('/<appid>/<dbname>/_bulk_docs', methods=['POST'])
def db_bulkdocs(appid, dbname):
    g.app = Application(appid, dbname)
    body = request.get_json()
    docs = body['docs']
    new_edits = body.get('new_edits', True)

    resp = []
    if new_edits:  # will happen only for _local
        local = []
        for i, doc in enumerate(docs):
            if doc['_id'].startswith('_local/'):
                ok, id, newrev = couch.local_put(doc['_id'][7:], doc)
                local.append(i)
                resp.append({
                    'ok': ok,
                    'id': id,
                    'rev': newrev
                } if ok else {'ok': False, 'id': id, 'error': 'error'})
            else:
                resp.append({'ok': False, 'error': 'unauthorized'})
        return jsonify(resp), 200

    else:  # the replicated docs to save
        resp, code = couch.bulkdocs(docs)
        return jsonify(resp), code


if __name__ == "__main__":
    app.run(port=settings.PORT, host='0.0.0.0')
