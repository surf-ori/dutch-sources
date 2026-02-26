# /// script
# dependencies = [
#     "marimo",
#     "pydantic-ai==1.58.0",
#     "requests==2.32.5",
# ]
# requires-python = ">=3.11"
# ///

import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import json
    import requests
    from collections import defaultdict

    return defaultdict, json, requests


@app.cell
def _(defaultdict, json, requests):
    ENDPOINT = "https://query.wikidata.org/sparql"

    QUERY = r"""
    SELECT ?goal ?goalLabel ?target ?targetLabel ?indicator ?indicatorLabel ?code WHERE {
      ?goal wdt:P31 wd:Q7649580 .
      ?target wdt:P361 ?goal .
      ?indicator wdt:P361 ?target .
      OPTIONAL { ?indicator wdt:P6845 ?code . }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?goalLabel ?targetLabel ?indicatorLabel
    """

    HEADERS = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "sdg-taxonomy-jsonld/1.0"
    }

    def main(out_path="sdg-taxonomy-wikidata.jsonld"):
        r = requests.get(ENDPOINT, params={"query": QUERY, "format": "json"}, headers=HEADERS, timeout=90)
        r.raise_for_status()
        data = r.json()

        # Collect nodes and edges
        goals = {}
        targets = {}
        indicators = {}

        goal_to_targets = defaultdict(set)
        target_to_indicators = defaultdict(set)

        for row in data["results"]["bindings"]:
            goal_uri = row["goal"]["value"]
            target_uri = row["target"]["value"]
            ind_uri = row["indicator"]["value"]

            goals[goal_uri] = row["goalLabel"]["value"]
            targets[target_uri] = row["targetLabel"]["value"]
            indicators[ind_uri] = {
                "label": row["indicatorLabel"]["value"],
                "code": row.get("code", {}).get("value")
            }

            goal_to_targets[goal_uri].add(target_uri)
            target_to_indicators[target_uri].add(ind_uri)

        # Build JSON-LD graph
        context = {
            "@vocab": "https://example.org/sdg-taxonomy/",
            "sdg": "https://example.org/sdg-taxonomy/",
            "schema": "http://schema.org/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "type": "@type",
            "label": "rdfs:label",
            "prefLabel": "skos:prefLabel",
            "hasTarget": {"@id": "sdg:hasTarget", "@type": "@id"},
            "hasIndicator": {"@id": "sdg:hasIndicator", "@type": "@id"},
            "source": "schema:isBasedOn",
            "indicatorCode": "sdg:indicatorCode",
        }

        graph = []

        # Goals
        for g_uri, g_label in goals.items():
            graph.append({
                "@id": g_uri,
                "type": "sdg:Goal",
                "prefLabel": {"@value": g_label, "@language": "en"},
                "hasTarget": sorted(goal_to_targets[g_uri]),
                "source": "https://www.wikidata.org/"
            })

        # Targets
        for t_uri, t_label in targets.items():
            graph.append({
                "@id": t_uri,
                "type": "sdg:Target",
                "prefLabel": {"@value": t_label, "@language": "en"},
                "hasIndicator": sorted(target_to_indicators[t_uri]),
                "source": "https://www.wikidata.org/"
            })

        # Indicators
        for i_uri, meta in indicators.items():
            node = {
                "@id": i_uri,
                "type": "sdg:Indicator",
                "prefLabel": {"@value": meta["label"], "@language": "en"},
                "source": "https://www.wikidata.org/"
            }
            if meta["code"]:
                node["indicatorCode"] = meta["code"]
            graph.append(node)

        out = {"@context": context, "@graph": graph}

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        print(f"Wrote {out_path} with {len(graph)} nodes")

    if __name__ == "__main__":
        main()
    return


if __name__ == "__main__":
    app.run()
