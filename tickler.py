#!/bin/env python3

import sys
import time
import socket
import logging
import argparse
import subprocess
from datetime import timedelta

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.metadata import protect_name
from cassandra import ConsistencyLevel, ReadTimeout

try:
    from tqdm import tqdm
except ImportError:
    print "This program looks better if you have TQDM installed (pip install tqdm)"
    def tqdm(iterator,total,unit="",mininterval=.5):
        """THIS IS NOT TQDM, THIS IS AN UGLY SHIM"""
        if total:
            ofkeys = "/"+str(int(total))
        else:
            ofkeys =""
        print_interval = 10000
        last = time.time()
        start_time = last

        for row_count,i in enumerate(iterator):
            if (row_count % print_interval) == 1:
                now = time.time()
                print  '{}{} rows processed ({} lines in {} seconds)'.format(str(row_count),ofkeys, print_interval, round(now-last,1))
                if total:
                    elapsed=now-start_time
                    speed=elapsed/row_count
                    print "   elapsed: {}, estimated total: {}".format(pretty_delta_seconds(elapsed),pretty_delta_seconds(speed*total))
                last=now
            yield i


def set_loglevel(n):
    levels=[
        logging.WARN,
        logging.INFO,
        logging.DEBUG,
        logging.NOTSET
     ]
    index=min(n or 0,len(levels))
    logging.basicConfig(level=levels[index])

# Get the arguments
def getopts(): 
    #Should be replaced with a real argparse
    parser = argparse.ArgumentParser(description='Read all rows in a key-space with consistency ALL in order to force read-repairs')
    cassandra = parser.add_argument_group('cassandra', 'cassandra options')
    cassandra.add_argument('-i','--ip', action='store', nargs='?',
            default="127.0.0.1", help='hostname of a cassandra node')
    cassandra.add_argument('-p','--port', action='store', nargs='?',
            default="9042", help='cassandra port')
    cassandra.add_argument('-t','--throttle', metavar="NS", action='store', nargs='?', type=int,
            default=0, help='Wait this many nano-seconds between database-queries')
    cassandra.add_argument('keyspace', action='store', help="Keyspace to be repaired")
    cassandra.add_argument('table', type=str, help="Table to be repaired")
    cassandra.add_argument('--keep-going', action='store_true',
            help='Attempt to keep going even if a row fails to repair. If you have to use this you have _problems_')

    printing = parser.add_argument_group('print options')
    printing.add_argument('--guess-time', action='store_true',
            help='Make a crude guess at the time when starting. Requires NodeTool locally. Probbably breaks horribly unless you are running cas-tickler from an actual node in the cluster (FIXME PLZ)')

    printing.add_argument('--verbose', '-v', action='count')

    args = parser.parse_args()
    
    set_loglevel(args.verbose)
    logging.info("keyspace "+ args.keyspace )
    logging.info("table "+ args.table )
    logging.info("ip "+ args.ip )
    logging.info("port "+ args.port )
    logging.info("throttle "+ str(args.throttle) )

    print_settings={ "guess_time":args.guess_time }
    cas_settings={ "keyspace": args.keyspace, "table": args.table, "ip": args.ip, "port": args.port, "throttle": args.throttle, "keep_going": args.keep_going}

    return {"keyspace": args.keyspace, "table": args.table, "ip": args.ip, "port": args.port, "throttle": args.throttle, 'cas_settings':cas_settings ,'print_settings':print_settings}

def connect(cass_keyspace,cass_ip="127.0.0.1", cass_port=9042):
    # Set the connections to the cluster
    cluster = Cluster(
        [cass_ip],
        port=cass_port, compression=False)

    # Connect to the KS
    return cluster.connect(cass_keyspace)

def prepare_all_keys_statement(cass_table, primary_key, consistency_level=ConsistencyLevel.QUORUM):
    # read every key of the table
    query = 'SELECT {} FROM {}'.format(protect_name(primary_key),protect_name(cass_table)) 
    return SimpleStatement(query, fetch_size=1000, consistency_level=consistency_level)

def prepare_repair_statement(cass_table, primary_key, session):
    repair_query = 'SELECT COUNT(1) FROM {} WHERE {} = ?'.format(protect_name(cass_table),
                                                                 protect_name(primary_key))
    repair_statement = session.prepare(repair_query)
    repair_statement.consistency_level = ConsistencyLevel.ALL
    return repair_statement

def get_network_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('<broadcast>', 0))
    return s.getsockname()[0]


def get_pct_ownership(keyspace):
    my_ip=str(get_network_ip())
    nodestats=subprocess.check_output(["nodetool", "status",protect_name(keyspace)])
    for i in nodestats.splitlines():
        if my_ip in i:
            for j in i.split(" "):
                if "%" in j:
                    return float(j[:-1])/100
    logging.warning("unable to calculate local ownership, assuming 100% ownership")
    return 1

def get_keycount(cas_settings):
    keyspace = cas_settings['keyspace']
    table = cas_settings['table']
    logging.info("calculating number of keys in table")

    tablestats=subprocess.check_output(["nodetool", "cfstats",protect_name(keyspace) +"."+ protect_name(table)])

    ownership=get_pct_ownership(keyspace)
    for i in tablestats.splitlines():
        if "Number of keys (estimate)" in i:
            own_keys=float(i.split(":")[1].strip())
            break
    num_keys=1/ownership*own_keys
    logging.info("nodetool reports {} keys and an ownership of {:%},giving a total of: {} keys".format(own_keys,ownership, num_keys))
    return num_keys


def pretty_delta_seconds(seconds):
    deltastr = str(timedelta(0,abs(seconds))).split(".")[0]
    if seconds >= 0:
        return deltastr
    else:
        return "-"+deltastr

def attempt_repair(primary_key, session, cas_settings, print_settings):
    cass_keyspace = cas_settings["keyspace"]
    cass_table = cas_settings["table"]

    all_keys_statement = prepare_all_keys_statement(cass_table, primary_key)
    repair_statement = prepare_repair_statement(cass_table, primary_key, session)
    idle_time = float(cas_settings["throttle"]) / 1000000

    if print_settings['guess_time']:
        num_keys=get_keycount(cas_settings)
    else:
        num_keys=None
    
    print 'Starting to repair table ' + cass_table
    for user_row in tqdm(session.execute(all_keys_statement),total=num_keys,unit="keys",mininterval=.5):
        logging.debug("reading row: " + repr(user_row) ) 
        try:
            session.execute(repair_statement, [user_row[0]])
            time.sleep(idle_time)  # delay in microseconds between reading each row
        except ReadTimeout as e:
            logging.error("Failed after"+repr(user_row))
            if cas_settings['keep_going']:
                print(e)
            else:
                raise
    print 'Repair of table {} '.format(cass_table, pretty_delta_seconds(time.time() - start_time))
    print str(row_count) + ' rows read and repaired'

def main():
    opts=getopts()
    session = connect(opts["keyspace"],opts["ip"],opts["port"])

    # get the primary key of the table
    # May have interesting behavior if the table has a composite PK
    primary_key = session.cluster.metadata.keyspaces[opts["keyspace"]].tables[opts["table"]].primary_key[0].name

    logging.info("primary key: "+  str(primary_key) )
    if primary_key:
        attempt_repair(primary_key, session, opts['cas_settings'], opts['print_settings'])
    else:
        logging.error("failed to get primary key for keyspace {} table {}".format(opts["keyspace"], opts["table"]))

if __name__ == "__main__":
    main()
