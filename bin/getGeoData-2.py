from pathlib import Path
import yaml
import apsw
import ipaddress
import os
import time
import functools
import time

# ---------------------------------------------------------------------
# This script will determine the direction of the traffic by
# getting the source IP and destination IP addresses
# if the source IP address is on the internal network
# and the destination IP address is NOT on the internal
# network, then we will have outbound traffic.
# the reverse will apply to inbound traffic and when
# both are on the internal network, then this will be
# internal traffic
# for inbound and outbound traffic we will look at the
# ip, calculate an ip2long and then do a look up against
# the geo.db database.
# --------------------------------------------------------------------

start = time.time()

with open(os.path.join(Path.cwd(), 'config.yaml'), 'r') as fin:
    CONFIG = yaml.load(fin, Loader=yaml.FullLoader)

dbLocation = CONFIG['db']['dbLocation']
print(dbLocation)

geodbLocation = CONFIG['db']['geodbLocation']
print(geodbLocation)

db_conn = apsw.Connection(dbLocation)
db_cur = db_conn.cursor()

geodb_conn = apsw.Connection(geodbLocation)
geodb_cur = geodb_conn.cursor()



start = time.time()
current_tm = time.time()

def memoize(func):
    cache = func.cache = {}
    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return memoized_func


@memoize
def get_country(iplong, count):
    retval = 'Unknown'
    global current_tm
    global start
    current_tm = time.time()
    count += 1
    sql2 = """
    SELECT country_name from Ip2Location where ip_from <= ? and ip_to >= ?
    """
    for new_row in geodb_cur.execute(sql2, (iplong, iplong)):
        country_name = new_row[0]
        if count % 1000 == 0: print(f'[+] {country_name} in {(current_tm-start):.2f} seconds')
        retval = country_name
    return retval, count



def update_db(cur, data):
    sql3 = """
                UPDATE deviceLog
                SET country_name = ?
                WHERE ID = ?
           """
    cur.execute('begin')
    cur.executemany(sql3, data)
    cur.execute('commit')


def generate_query_data(sql):
    update_data = []
    count = 0
    for row in db_cur.execute(sql):
        ID, iplong = row
        country_name, count = get_country(iplong, count)
        update_data.append([country_name, ID])
    update_db(cur=db_cur, data=update_data)





sql = """
    select id, srcIPLong as iplong from deviceLog where direction = 'inbound'
"""
generate_query_data(sql=sql)

sql = """
    select id, dstIPLong as iplong from deviceLog where direction = 'outbound'
"""
generate_query_data(sql=sql)


end = time.time()
print(f'[+] Program completed in {(end-start):.2f} seconds')