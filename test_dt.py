from tools.tools import now, fromISO
import datetime as dt
import iso8601


t = '2017-10-19T11:55:39.860'
print('@ ', t)
t2 = fromISO(t)
print('fromISO ', t2 )
t3 = iso8601.parse_date(t)
print('from iso8601', t3)
n = dt.datetime.now()
print('now', fromISO(n.isoformat()))
print('now', n)
print(fromISO(n.isoformat()) - t2 )
