import psycopg2

import settings


conn = psycopg2.connect(settings.DATABASE_URL)


def pg():
    return conn.cursor()


def commit():
    conn.commit()
