import sys
import time
import logging
import subprocess
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.metadata import protect_name
from cassandra import ConsistencyLevel

logging.basicConfig(level=logging.WARN)
# Get the arguments
def getopts(): 
    #Should be replaced with a real argparse
    if len(sys.argv) == 6:
        cass_keyspace = sys.argv[1]  # Cassandra Keyspace containing the Table to tickle
        cass_table = sys.argv[2]  # Cassandra Table to tickle
        cass_ip = sys.argv[3]  # Cassandra Port
        cass_port = sys.argv[4]  # CQL Port
        cass_throttle = sys.argv[5]  # microseconds

    guess_time=True
    else:
        logging.error("usage: tickler.py [Keyspace] [Table] [Cluster IP] [Port] [microseconds between each read]")
        sys.exit(1)
    logging.info("keyspace "+ cass_keyspace )
    logging.info("table "+ cass_table )
    logging.info("ip "+ cass_ip )
    logging.info("port "+ cass_port )
    logging.info("throttle "+ cass_throttle )

    return {"keyspace": cass_keyspace, "table": cass_table, "ip": cass_ip, "port": cass_port, "throttle": cass_throttle, 'guess_time':true}

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

def attempt_repair(primary_key, cass_keyspace, cass_table, session, throttle, print_interval = 1000, guess_time=false):
    all_keys_statement = prepare_all_keys_statement(cass_table, primary_key)
    repair_statement = prepare_repair_statement(cass_table, primary_key, session)
    idle_time = float(throttle) / 1000000
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
                print  '{} rows processed ({} lines in {:.2} seconds)'.format(str(row_count), print_interval, now-last)
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
        attempt_repair(primary_key, opts["keyspace"], opts["table"], session, opts["throttle"], opts['guess_time'])

if __name__ == "__main__":
    main()
