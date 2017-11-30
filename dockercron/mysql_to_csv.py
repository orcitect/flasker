import sys
import os.path
import time
import logging
import pymysql
import unicodecsv as csv

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filemode='w',
    filename='app.log',
    level=logging.INFO
)

logger = logging.getLogger()

db = {
    'user': 'nerd_ext',
    'password': 'Netent4ever!',
    'host': 'cha-nerd-01.nix.csmodule.com',
    'port': 3306,
    'database': 'nerd',
    'charset': 'utf8'
}

connection = pymysql.connect(**db)
if connection:
    print "connection successful" 

cutoff = time.time() - 1 * 60
fname = 'results.csv'
regen = False

try:
    with open(fname, 'r') as f:
        if os.path.getmtime(fname) < cutoff:
            print "Cache is old, timestamp:", time.ctime(os.path.getmtime(fname))
            regen = True
        else:
            print "Cache is fresh, timestamp:", time.ctime(os.path.getmtime(fname))
except IOError:
    print "Error opening %s: regenerating cache" % fname
    regen = True

if regen:
    try:
        with connection.cursor() as cursor:
            query = "SELECT * FROM nerd"
            cursor.execute(query)
            writer = csv.writer(open(fname, 'wt'))
            writer.writerow([i[0] for i in cursor.description])
            writer.writerows(cursor)
            logger.info('cache refreshed')
            print "Wrote %s rows to csv. (%s KB)" % (cursor.rowcount, os.path.getsize(fname) >> 10)
    finally:
        connection.close()



class update_cache():
    try:
        with open(fname, 'r'):
            if os.path.getmtime(fname) < cutoff:
                print("Cache is old, timestamp:"), time.ctime(os.path.getmtime(fname))
                regen = True
            else:
                print("Cache is fresh, timestamp:"), time.ctime(os.path.getmtime(fname))
    except IOError:
        print("Error opening %s: regenerating cache" % fname)
        regen = True
    if(regen):
        try:
            with connection.cursor() as cursor:
                query = "SELECT * FROM nerd"
                cursor.execute(query)
                writer = csv.writer(open(fname, 'wt'))
                writer.writerow([i[0] for i in cursor.description])
                writer.writerows(cursor)
                logger.info('cache refreshed')
                print "Wrote %s rows to csv. (%s KB)" % (cursor.rowcount, os.path.getsize(fname) >> 10)
        finally:
            connection.close()
