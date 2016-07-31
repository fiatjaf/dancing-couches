import json
import psycopg2
import datetime
from flask import request, g, abort

from pg import pg, commit, rollback
from utils import makerev, parse_date


def local_put(id, doc):
    with pg() as c:
        newrev = makerev(doc.pop('_rev', '0-'))
        id = doc.pop('_id')[7:]

        try:
            c.execute('''
INSERT INTO localdocs
(appid, dbname, id, rev, doc)
VALUES (%s, %s, %s, %s, %s)
            ''', (g.app.appid, g.app.dbname, id, newrev, json.dumps(doc)))
            commit()
        except psycopg2.ProgrammingError:
            rollback()
            return False, '', ''

        return True, '_local/' + id, newrev
    return False, '', ''


def local_get(id):
    with pg() as c:
        c.execute('''
SELECT doc, rev
FROM localdocs
WHERE appid = %s AND dbname = %s AND id = %s
        ''', (g.app.appid, g.app.dbname, id))
        row = c.fetchone()
        if not row:
            abort(404)

        doc = row[0]
        doc['_id'] = '_local/' + id
        doc['_rev'] = row[1]
        return doc


def changes(since, limit):
    # translate the 'since' parameter from a couchdb seq number
    # to a unix timestamp.
    timestamp = 0
    with pg() as c:
        c.execute('''
SELECT timestamp
FROM checkpoints
WHERE appid = %s AND dbname = %s AND seq <= %s
ORDER BY seq
LIMIT 1
        ''', (g.app.appid, g.app.dbname, since))
        row = c.fetchone()
        if row:
            timestamp = row[0]

        # fetch updated rows since the given timestamp from the third-party app
        changed_rows = g.app.changes_since(timestamp, request.authorization)
        rowsbyid = {}
        ids = set()
        for row in changed_rows:
            row['last_update'] = parse_date(row['last_update'])
            rowsbyid[row['id']] = row
            ids.add(row['id'])

        # check which of these rows has a stored rev and/or has changed
        if ids:
            c.execute('''
SELECT
DISTINCT ON (id) id, rev, last_update
FROM revstore
WHERE appid = %s AND dbname = %s AND id IN %s
ORDER BY id, global_seq DESC
            ''', (g.app.appid, g.app.dbname, tuple(ids)))
            not_new = c.fetchall()
        else:
            not_new = []

        # make new revs for everything that has actually changed
        # and set the revs for the things that have not changed
        to_insert = set()
        for id, rev, last_update in not_new:
            if last_update != rowsbyid[id]['last_update']:
                rowsbyid[id]['rev'] = makerev(rev)
                to_insert.add(id)
            else:
                rowsbyid[id]['rev'] = rev

            # this will make 'ids' contain only the rowsbyid without rev
            # namely, the new ones.
            ids.remove(id)

        # make revs for everything that doesn't have a rev
        for id in ids:
            rowsbyid[id]['rev'] = makerev()
            to_insert.add(id)

        # store the new revs and get new seq numbers for them
        if to_insert:
            to_revstore = []
            for id in to_insert:
                r = rowsbyid[id]
                last = r['last_update']
                sql = c.mogrify(
                    '(%s, %s, %s, %s, %s)',
                    (g.app.appid, g.app.dbname, r['id'], r['rev'], last)
                )

                to_revstore.append(sql)
            insertedrevs = b','.join(to_revstore).decode('utf-8')

            c.execute('''
INSERT INTO revstore
(appid, dbname, id, rev, last_update)
VALUES ''' + insertedrevs)

        c.execute('''
SELECT
  id,
  rev,
  row_number() OVER (PARTITION BY appid, dbname ORDER BY global_seq) AS seq
FROM revstore
WHERE appid = %s AND dbname = %s
LIMIT %s
        ''', (g.app.appid, g.app.dbname, limit))

        # turn these into actual couchdb changes, giving them seq numbers
        couch_changes = [
            {'seq': seq, 'id': id, 'changes': [{'rev': rev}]}
            for id, rev, seq in c if seq > since
        ]

        # the last seq must be fetched independently
        last_seq = fetch_last_seq(c, g.app.appid, g.app.dbname)

        # save the last seq as a checkpoint (if not saved yet)
        c.execute('''
INSERT INTO checkpoints (appid, dbname, seq)
VALUES (%s, %s, %s)
ON CONFLICT DO NOTHING
        ''', (g.app.appid, g.app.dbname, last_seq))

        commit()
        return {
            'results': couch_changes,
            'last_seq': last_seq
        }


def revsdiff(docrevs):
    with pg() as c:
        fetch = []
        missing = {}
        for id, revs in docrevs.items():
            missing[id] = {'missing': []}
            for rev in revs:
                fetch.append((id, rev))
                missing[id]['missing'].append(rev)

        c.execute('''
SELECT id, rev
FROM revstore
WHERE (id, rev) IN %s
        ''', (tuple(fetch or '____'),))

        for id, rev in c:
            missing[id]['missing'].remove(rev)

        return missing


def alldocs(ids):
    with pg() as c:
        # fetch revs from db
        c.execute('''
SELECT
DISTINCT ON (id) id, rev
FROM revstore
WHERE id IN %s
ORDER BY id, global_seq DESC
        ''', (tuple(ids or '____'),))

        revmap = {}
        for id, rev in c:
            revmap[id] = rev

        # fetch docs from the application and build a couchdb-like response
        rows = []
        for doc in g.app.fetch_docs(ids, request.authorization):
            id = doc.pop('id')
            rev = revmap[id]
            doc['_id'] = id
            doc['_rev'] = rev
            row = {'id': id, 'key': id, 'value': {'_rev': rev}, 'doc': doc}
            rows.append(row)

        return {
            'offset': 0,
            'rows': rows,
            'total_rows': len(rows)
        }


def bulkget(idrevs):
    pairs = []
    notfound = set()
    for idrev in idrevs:
        pairs.append((idrev['id'], idrev['rev']))
        notfound.add((idrev['id'], idrev['rev']))

    with pg() as c:
        # from all the requested id-rev pairs, check which ones represent
        # the last updated version of a doc.
        if pairs:
            c.execute('''
SELECT id, rev
FROM (
  SELECT
  DISTINCT ON (id) id, rev
  FROM revstore
  ORDER BY id, global_seq DESC
)t WHERE (id, rev) IN %s
            ''', (tuple(pairs),))
        revmap = {}
        for id, rev in c:
            revmap[id] = rev

        # then fetch these from the application
        idstofetch = revmap.keys()
        fetched = g.app.fetch_docs(idstofetch, request.authorization)

        # and build a couchdb-like response
        results = []
        resultsidmap = {}
        for index, doc in enumerate(fetched):
            id = doc.pop('id')
            rev = revmap[id]
            doc['_id'] = id
            doc['_rev'] = rev
            result = {'id': id, 'docs': [{'ok': doc}]}
            results.append(result)
            resultsidmap[id] = index
            notfound.remove((id, rev))

        # return errors for all docs we didn't fetch
        for id, rev in notfound:
            index = resultsidmap.get(id)
            if not index:
                result = {'id': id, 'docs': []}
                resultsidmap[id] = len(results)
                results.append(result)
            else:
                result = results[index]
                error = {'error': 'not_found', 'reason': 'deleted'}
                result['docs'].append({'error': error})

        return {
            'results': results
        }


def bulkdocs(docs):
    with pg() as c:
        # discover which of the docs sent here are winning revs
        docsbyid = {}
        to_insert = []
        for doc in docs:
            docsbyid[doc['_id']] = docsbyid.get(doc['_id'], [])
            docsbyid[doc['_id']].append(doc)
            to_insert.append((doc['_id'], doc['_rev']))
        maybe_winning = []
        for id, docs in docsbyid.items():
            # first we get the bigger rev of the revs sent for each document
            print(docs)
            local_winning = sorted(docs, key=lambda x: x['_rev'])[-1]
            maybe_winning.append(local_winning)

        # then we check the database to see if there isn't anything bigger
        c.execute('''
SELECT
DISTINCT (id) id, rev, global_seq
FROM revstore
WHERE appid = %s AND dbname = %s AND id IN %s
ORDER BY id, global_seq DESC
        ''', (g.app.appid, g.app.dbname, tuple(docsbyid.keys())))
        stored_winning = {}
        for id, rev, _ in c:
            stored_winning[id] = int(rev.split('-')[0])

        # if there is -- then the app already has the winning rev
        # otherwise -- we must send this new rev so the app will be updated
        to_update = []
        for doc in maybe_winning:
            storedw = stored_winning.get(doc['_id'], 0)
            localw = int(doc['_rev'].split('-')[0])
            if storedw <= localw:
                to_update.append(doc)

        # send to the app only the docs that need some kind of update
        # -- and only the most recent version --
        saved = g.app.save(to_update)
        if saved is False:
            # in case of failure, we will not store anything
            return {'error': 'error'}, 503
        elif type(saved) is list:
            # TODO if the doc wasn't handled properly by the app,
            # TODO do not store its rev.
            resp = [{'ok': ok, 'id': doc['_id'], 'rev': doc['_rev']}
                    for ok, doc in zip(saved, docs)], 200
        else:
            resp = [{'ok': True, 'id': doc['_id'], 'rev': doc['_rev']}
                    for doc in docs], 200

        # then we update our revstore with the new revs
        if to_insert:
            to_revstore = []
            for id, rev in to_insert:
                last = datetime.datetime.now()
                sql = c.mogrify(
                    '(%s, %s, %s, %s, %s)',
                    (g.app.appid, g.app.dbname, id, rev, last)
                )

                to_revstore.append(sql)
            insertedrevs = b','.join(to_revstore).decode('utf-8')
            c.execute('''
INSERT INTO revstore
(appid, dbname, id, rev, last_update)
VALUES ''' + insertedrevs)
            commit()

        return resp


def info():
    with pg() as c:
        return {
            'committed_update_seq': 0,
            'compact_running': False,
            'data_size': 99999999999,
            'db_name': 'source',
            'disk_format_version': 6,
            'disk_size': 99999,
            'doc_count': 99999,
            'doc_del_count': 999,
            'instance_start_time': '000000000',
            'purge_seq': 0,
            'update_seq': fetch_last_seq(c, g.app.appid, g.app.dbname)
        }


def fetch_last_seq(cursor, appid, dbname):
    cursor.execute('''
SELECT seq FROM (
  SELECT
    row_number() OVER (PARTITION BY appid, dbname ORDER BY global_seq) AS seq
  FROM revstore WHERE appid = %s AND dbname = %s
)y
ORDER BY seq DESC
LIMIT 1
    ''', (appid, dbname))
    r = cursor.fetchone()
    return r[0] if r else 0
