# pip install graphdatascience
from graphdatascience import GraphDataScience
# Connect to the database
host = "bolt://127.0.0.1:7687"
user = "neo4j"
password= "Mbg!234567"
gds = GraphDataScience(host, auth=(user, password), database="neo4j")

# Load dataset to Neo4j
imp = gds.run_cypher("""
SHOW SETTINGS YIELD name, value
WHERE name = 'server.directories.import'
RETURN value
""")
print(imp)  # this DataFrame shows the absolute path of the import dir

from neo4j import GraphDatabase
import pandas as pd

driver = GraphDatabase.driver(host, auth=(user, password), database="neo4j")

with driver.session() as s:
    s.run("""
        CREATE CONSTRAINT station_id IF NOT EXISTS
        FOR (s:Station) REQUIRE s.id IS UNIQUE
    """)

for chunk in pd.read_csv("/home/mahdi/Knowledge-Graph/nr-stations-all.csv", chunksize=10_000):
    rows = chunk.to_dict("records")
    with driver.session() as s:
        s.execute_write(
            lambda tx, r: tx.run("""
            UNWIND $rows AS row
            MERGE (s:Station {id: row.id})
            SET  s.name = row.name,
                 s.lat  = toFloat(row.lat),
                 s.lon  = toFloat(row.lon)
            """, rows=r),
            rows
        )


# # 1) constraint/index (idempotent)
# gds.run_cypher("""
# CREATE CONSTRAINT station_id IF NOT EXISTS
# FOR (s:Station) REQUIRE s.id IS UNIQUE
# """)

# # 2) import (local server: file:///; Aura: https://)
# url = "file:///nr-stations-all.csv"  # e.g., import/stations/nr-stations-all.csv
# # url = "https://your-host/stations/nr-stations-all.csv"  # Aura

# gds.run_cypher("""
# LOAD CSV WITH HEADERS FROM $url AS row
# MERGE (s:Station {id: row.id})
# SET  s.name = row.name,
#      s.lat  = toFloat(row.lat),
#      s.lon  = toFloat(row.lon)
# """, params={"url": url})




# # Load stations as nodes
# gds.run_cypher(
# """
# LOAD CSV WITH HEADERS FROM "nr-stations-all.csv" AS station
# CREATE (:Station {name: station.name, crs: station.crs})
# """
# )
# # Load tracks bewteen stations as relationships
# gds.run_cypher(
# """
# LOAD CSV WITH HEADERS FROM "nr-station-links.csv" AS track
# MATCH (from:Station {crs: track.from})
# MATCH (to:Station {crs: track.to})
# MERGE (from)-[:TRACK {distance: round( toFloat(track.distance), 2 )}]->(to)
# """
# )
gds.close()