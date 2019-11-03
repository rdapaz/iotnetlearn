from pathlib import Path
import os
import apsw
import csv
import time


def drop_tables():
    sql = """
    drop table if exists ip2Location;
    """
    cur.execute(sql)


def create_tables(arr):
    sql = """
    create table if not exists ip2Location
    (
        id              integer primary key,
        ip_from         integer,
        ip_to           integer,
        country_code    text,
        country_name    text
    );
    
    insert into ip2Location
    (ip_from, ip_to, country_code, country_name)
    values (?, ?, ?, ?);
    """
    cur.executemany(sql, arr)
    cur.execute("VACUUM")


def clear_tables():
    sql = """
    delete from ip2Location;
    """
    cur.execute(sql)
    cur.execute("VACUUM")


if __name__ == '__main__':
    start = time.time()
    cwd = Path.cwd()
    db_path = os.path.join(cwd.parent, 'db', 'geodb', 'geo.db')
    csv_path = os.path.join(cwd.parent, 'data', 'IP2LOCATION-LITE-DB1.CSV')
    global cur
    conn = apsw.Connection(db_path)
    cur = conn.cursor()
    fields = ['ip_from', 'ip_to', 'country_code', 'country_name']
    with open(csv_path) as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=',', fieldnames=fields)
        arr = []
        for row in csv_reader:
            arr.append([
                row['ip_from'],
                row['ip_to'],
                row['country_code'],
                row['country_name']
                ])
    drop_tables()
    create_tables(arr)
    end = time.time()
    print(f'[+] Program completed in {(end-start):.2f} seconds')