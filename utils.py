import cuid
import datetime
from dateutil import parser


def makerev(oldrev=None):
    if oldrev:
        oldnumber = oldrev.split('-')[0]
        newnumber = str(int(oldnumber) + 1)
    else:
        newnumber = '1'
    return newnumber + '-' + cuid.slug()


def parse_date(raw):
    try:
        return datetime.datetime.fromtimestamp(raw)
    except TypeError:
        return parser.parse(raw)
