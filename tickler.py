import sys
import time
import logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.metadata import protect_name
from cassandra import ConsistencyLevel

logging.basicConfig(level=logging.WARN)
# Get the arguments
def main():
    cass_keyspace = sys.argv[1]  # Cassandra Keyspace containing the Table to tickle
    cass_table = sys.argv[2]  # Cassandra Table to tickle
    cass_ip = sys.argv[3]  # Cassandra Port
    cass_port = sys.argv[4]  # CQL Port
    cass_throttle = sys.argv[5]  # microseconds

    logging.info("keyspace "+ cass_keyspace )
    logging.info("table "+ cass_table )
    logging.info("ip "+ cass_ip )
    logging.info("port "+ cass_port )
    logging.info("throttle "+ cass_throttle )

    # Set the connections to the cluster
    cluster = Cluster(
        [cass_ip],
        port=cass_port, compression=False)

    # Connect to the KS
    session = cluster.connect(cass_keyspace)


    # get the primary key of the table
    # May have interesting behavior if the table has a composite PK
    primary_key = cluster.metadata.keyspaces[cass_keyspace].tables[cass_table].primary_key[0].name
    logging.info("primary key: "+  str(primary_key) )

    if primary_key:
        # read every key of the table
        query = 'SELECT {} FROM {}'.format(protect_name(primary_key),protect_name(cass_table)) 
        statement = SimpleStatement(query, fetch_size=1000, consistency_level=ConsistencyLevel.QUORUM)
        print 'Starting to repair table ' + cass_table
        repair_query = 'SELECT COUNT(1) FROM {} WHERE {} = ?'.format(protect_name(cass_table),
                                                                     protect_name(primary_key))
        repair_statement = session.prepare(repair_query)
        repair_statement.consistency_level = ConsistencyLevel.ALL
        row_count = 0
        last = time.time()
        start_time = last
        print_interval = 1000
        idle_time = float(cass_throttle) / 1000000
        for user_row in session.execute(statement):
            logging.debug("reading row: " + repr(user_row) ) 
            row_count += 1
            session.execute(repair_statement, [user_row[0]])
            time.sleep(idle_time)  # delay in microseconds between reading each row
            if (row_count % print_interval) == 0:
                now = time.time()
                print  '{} rows processed ({} lines in {:.2} seconds)'.format(str(row_count), print_interval, now-last)
                last=now

        print 'Repair of table ' + cass_table + ' {} seconds'.format(time.time() - start_time)
        print str(row_count) + ' rows read and repaired'

if __name__ == "__main__":
    main()
