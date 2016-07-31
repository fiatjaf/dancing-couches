import json
import copy
import requests

from pg import pg


class NotFound(Exception):
    pass


class Application(object):
    def __init__(self, appid, dbname):
        self.appid = appid
        self.dbname = dbname

        with pg() as c:
            c.execute('''
SELECT endpoint FROM app_dbs WHERE appid = %s AND dbname = %s
            ''', (appid, dbname))

            r = c.fetchone()
            if not r:
                raise NotFound

            self.endpoint = r[0]

    def changes_since(self, timestamp, auth):
        r = requests.get(self.endpoint + '/UPDATED', params={
            'username': auth and auth.username,
            'password': auth and auth.password,
            'timestamp': timestamp
        })
        if r.ok:
            return r.json()
        return []

    def fetch_docs(self, ids, auth):
        r = requests.get(self.endpoint + '/FETCH', params={
            'username': auth and auth.username,
            'password': auth and auth.password,
            'ids': ','.join(ids)
        })
        if r.ok:
            return r.json()
        return []

    def save(self, docs):
        create = []
        update = []
        delete = []
        for doc in docs:
            doc = copy.copy(doc)
            doc['id'] = doc.pop('_id')
            rev = doc.pop('_rev')
            if doc.pop('_deleted', False):
                delete.append(doc)
            elif rev.startswith('1-'):
                create.append(doc)
            else:
                update.append(doc)

        r = requests.post(self.endpoint + '/SAVE',
                          data=json.dumps({
                              'create': create,
                              'update': update,
                              'delete': delete
                          }),
                          headers={'Content-Type': 'application/json'})
        if not r.ok:
            return False
        return r.json()
