#!/usr/bin/env python

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "python_ctr"

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS clickimpression (
            OwnerId int,
            AdId int,
            numClicks int,
            numImpressions int,
            PRIMARY KEY (OwnerId, AdId)
        )
        """)

    query = SimpleStatement("""
        INSERT INTO clickimpression (OwnerId, AdId, numClicks, numImpressions)
        VALUES (%(oi)s, %(ai)s, %(nc)s, %(ni)s)
        """, consistency_level=ConsistencyLevel.ONE)

    prepared = session.prepare("""
        INSERT INTO clickimpression (OwnerId, AdId, numClicks, numImpressions)
        VALUES (?, ?, ?, ?)
        """)
        
    vals = [[1,1,1,10], [1,2,0,5], [1,3,1,20], [1,4,0,15],
            [2,1,0,10], [2,2,0,55], [2,3,0,13], [2,4,0,21],
            [3,1,1,32], [3,2,0,23], [3,3,2,44], [3,4,1,36]]
    for row in vals:
        session.execute(query, dict(oi=row[0], ai=row[1], nc=row[2], ni=row[3]))
        session.execute(prepared, (row[0], row[1], row[2], row[3]))

    future = session.execute_async("SELECT * FROM clickimpression")
    rows = future.result()

    print "Find the ctr for each OwnerId, AdId pair:"
    print "OwnerId\tAdId\tCTR"
    for row in rows:
        print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]/float(row[3]))
        
    print "\nCompute the ctr for each OwnerId:"
    print "OwnerId\tCTR"
    clicks = {}
    impressions = {}
    for row in rows:
        if row[0] in clicks:
            clicks[row[0]] += row[2]
        else:
            clicks[row[0]] = row[2]
        if row[0] in impressions:
            impressions[row[0]] += row[3]
        else:
            impressions[row[0]] = row[3]
    for owner in clicks:
        print str(owner) + "\t" + str(clicks[owner]/float(impressions[owner]))
        
    future = session.execute_async("SELECT * FROM clickimpression WHERE OwnerId = 1 AND AdId = 3")
    rows = future.result()
            
    print "\nCompute the ctr for OwnerId = 1, AdId = 3:"
    print "OwnerId\tAdId\tCTR"
    for row in rows:
        print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]/float(row[3]))
        
    future = session.execute_async("SELECT * FROM clickimpression WHERE OwnerId = 2")
    rows = future.result()
    
    print "\nCompute the ctr for OwnerId = 2:"
    print "OwnerId\tCTR"
    clicks = 0;
    impressions = 0;
    for row in rows:
        clicks += row[2]
        impressions += row[3]
    print str(rows[0][0]) + "\t" + str(clicks/float(impressions))
    
    session.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main()
