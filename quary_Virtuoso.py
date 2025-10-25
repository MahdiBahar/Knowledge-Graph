from SPARQLWrapper import SPARQLWrapper, JSON

# Virtuoso endpoint (your local SPARQL endpoint)
sparql = SPARQLWrapper("http://localhost:8890/sparql")

# Example query: list 10 triples
sparql.setQuery("""
    SELECT ?s ?p ?o
    FROM <http://dbpedia.org>
    WHERE { ?s ?p ?o }
    LIMIT 10
""")

sparql.setReturnFormat(JSON)
results = sparql.query().convert()

# Print the results
for result in results["results"]["bindings"]:
    s = result["s"]["value"]
    p = result["p"]["value"]
    o = result["o"]["value"]
    print(s, p, o)
