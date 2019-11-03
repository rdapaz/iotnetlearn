from pathlib import Path
import yaml
import apsw
import ipaddress
import os
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


def is_internal_network_pred(*args):
    """
    This function is called from within sqlite
    and returns TRUE when the ip address matches
    those on the internal network
    """
    ip = args[0]
    network = args[1]
    ip = ipaddress.ip_address(ip)
    network = ipaddress.ip_network(network, strict=True)
    return ip in network


def ip2long(ip):
    ip_parts = [int(x) for x in ip.split('.')]
    octet1, octet2, octet3, octet4 = ip_parts
    return ( 256*256*256 *  octet1 + \
                256*256  *  octet2 + \
                    256  *  octet3 + \
                            octet4
           )

with open(os.path.join(Path.cwd(), 'config.yaml'), 'r') as fin:
    CONFIG = yaml.load(fin, Loader=yaml.FullLoader)

dbLocation = CONFIG['db']['dbLocation']
print(dbLocation)

geodbLocation = CONFIG['db']['geodbLocation']
print(geodbLocation)

db_conn = apsw.Connection(dbLocation)
db_conn.createscalarfunction('on_network', is_internal_network_pred, 2)
db_cur = db_conn.cursor()

geodb_conn = apsw.Connection(geodbLocation)
geodb_cur = geodb_conn.cursor()


def update_db(cur, data):
    sql3 = """
                UPDATE deviceLog
                SET srcIPLong = ?, dstIPLong = ?, direction = ?
                WHERE ID = ?
           """
    cur.execute('begin')
    cur.executemany(sql3, data)
    cur.execute('commit')


def generate_query_data(cur, sql, direction):
    home_network = CONFIG['home_network']['network_address']
    update_data = []
    for row in cur.execute(sql,(home_network, home_network)):
        id, srcAddr, dstAddr = row
        dstIPLong = ip2long(dstAddr)
        srcIPLong = ip2long(srcAddr)
        print(srcIPLong, dstIPLong, direction)
        update_data.append([srcIPLong, dstIPLong, direction, id])
    update_db(cur=cur, data=update_data)


sql = """
    -- We pass the network_address by parameter as sqlite does not support
    -- variables being declared
    SELECT id, srcAddr, dstAddr from deviceLog where on_network(srcAddr,?) IS TRUE and on_network(dstAddr, ?) IS FALSE
    """
direction = 'outbound'
generate_query_data(cur=db_cur, sql=sql, direction=direction)


sql = """
    -- We pass the network_address by parameter as sqlite does not support
    -- variables being declared
    SELECT id, srcAddr, dstAddr from deviceLog where on_network(srcAddr,?) IS FALSE and on_network(dstAddr, ?) IS TRUE
    """
direction = 'inbound'
generate_query_data(cur=db_cur, sql=sql, direction=direction)


sql = """
    -- We pass the network_address by parameter as sqlite does not support
    -- variables being declared
    SELECT id, srcAddr, dstAddr from deviceLog where on_network(srcAddr,?) IS TRUE and on_network(dstAddr, ?) IS TRUE
    """
direction = 'internal'
generate_query_data(cur=db_cur, sql=sql, direction=direction)

end = time.time()
print(f'[+] Program completed in {(end-start):.2f} seconds')