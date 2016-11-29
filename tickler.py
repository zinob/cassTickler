#!/bin/env python3

import sys
import time
import logging
import argparse
import subprocess
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.metadata import protect_name
from cassandra import ConsistencyLevel

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
            default=50, help='Wait this many nano-seconds between database-queries')
    cassandra.add_argument('keyspace', action='store', help="Keyspace to be repaired")
    cassandra.add_argument('table', type=str, help="Table to be repaired")

    printing = parser.add_argument_group('print options')
    printing.add_argument('-n','--status-interval', action='store', nargs='?',
            type=int, default=1000, help='Print status information after every n requested row')
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

    print_settings={ "guess_time":True,"print_interval": 1000}

    return {"keyspace": args.keyspace, "table": args.table, "ip": args.ip, "port": args.port, "throttle": args.throttle, 'print_settings':print_settings}

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

def get_keycount(keyspace,table):
    tablestats=subprocess.check_output(["nodetool", "cfstats",protect_name(keyspace) +"."+ protect_name(table)])
    for i in tablestats.splitlines():
        if "Number of keys (estimate)" in i:
            return float(i.split(":")[1].strip())

def attempt_repair(primary_key, cass_keyspace, cass_table, session, throttle, print_settings):
    all_keys_statement = prepare_all_keys_statement(cass_table, primary_key)
    repair_statement = prepare_repair_statement(cass_table, primary_key, session)
    idle_time = float(throttle) / 1000000
    print_interval=print_settings['print_interval']

    if print_settings['guess_time']:
        num_keys=get_keycount(cass_keyspace,cass_table)
        ofkeys="/"+str(int(num_keys))
    else:
        num_keys=False
        ofkeys=""
    
    last = time.time()
    start_time = last
    row_count = 0
    print 'Starting to repair table ' + cass_table
    try:
        for user_row in session.execute(all_keys_statement):
            logging.debug("reading row: " + repr(user_row) ) 
            row_count += 1
            session.execute(repair_statement, [user_row[0]])
            time.sleep(idle_time)  # delay in microseconds between reading each row
            if (row_count % print_interval) == 0:
                now = time.time()
                print  '{}{} rows processed ({} lines in {:.2} seconds)'.format(str(row_count),ofkeys, print_interval, now-last)

                if num_keys:
                    elapsed=now-start_time
                    speed=elapsed/row_count
                    print "   {}s elapsed {}s remaining".format(elapsed,speed*(num_keys-row_count))
                    
                last=now
    except:
        logging.error("Failed after"+repr(user_row))
        raise
    print 'Repair of table ' + cass_table + ' {} seconds'.format(time.time() - start_time)
    print str(row_count) + ' rows read and repaired'

def main():
    opts=getopts()
    session = connect(opts["keyspace"],opts["ip"],opts["port"])

    # get the primary key of the table
    # May have interesting behavior if the table has a composite PK
    primary_key = session.cluster.metadata.keyspaces[opts["keyspace"]].tables[opts["table"]].primary_key[0].name

    logging.info("primary key: "+  str(primary_key) )
    if primary_key:
        attempt_repair(primary_key, opts["keyspace"], opts["table"], session, opts["throttle"], opts['print_settings'])
    else:
        logging.error("failed to get primary key for keyspace {} table {}".format(opts["keyspace"], opts["table"]))

if __name__ == "__main__":
    main()
