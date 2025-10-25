import pyodbc

#It doesnt work. The virtuoso-opensource-7-odbc package isn’t present in the Ubuntu/Debian repositories by that exact name.
# It should be installed manually. Now, I prefer to load .ttl files via isql in virtuoso
# So, for now, ignore this code.
# Connect to Virtuoso ISQL interface
connection = pyodbc.connect(
    "Driver=Virtuoso Driver;HOST=localhost:1111;UID=dba;PWD=dba",
    autocommit=True
)
cursor = connection.cursor()

# Path to your TTL file
rdf_path = "/home/mahdi/Knowledge-Graph/data/RDF-data/"
rdf_file = "mappingbased-objects_lang=en.ttl"
graph_uri = "http://dbpedia.org"

# Register and load the RDF file
cursor.execute(f"ld_dir('{rdf_path}', '{rdf_file}', '{graph_uri}');")
cursor.execute("rdf_loader_run();")

print("✅ RDF file loading command sent to Virtuoso.")

# Optional: verify
cursor.execute("SPARQL SELECT COUNT(*) AS ?triples WHERE { ?s ?p ?o };")
for row in cursor.fetchall():
    print("Total triples:", row)

cursor.close()
connection.close()
