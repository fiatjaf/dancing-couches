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

    def user_allowed(self, auth):
        r = requests.get(self.endpoint + '/USER_ALLOWED', params={
            'username': auth and auth.username,
            'password': auth and auth.password
        })
        return r.ok

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
