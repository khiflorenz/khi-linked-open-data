# AUXILIARY FUNCTIONS FOR SPARQL QUERIES IN STEP 2
def build_sparql_query(prefix, values):
    '''
    Builds SPARQL query based on identifier. GNDs, ULANs, and VIAFs are mapped to Wikidata entities,
    then Wikidata entities are used to retrieve additional missing GNDs, ULANs, and VIAFs.
    '''
    # Common SELECT clause
    select_clause = f"""
    SELECT ?{prefix} ?wd WHERE {{
    """

    # Prepare VALUES clause based on the prefix
    if prefix == 'gnd':
        values_clause = f'VALUES ?gnd {{{values}}}'
        query_section = """
            ?wd wdt:P227 ?gnd.
        """
    elif prefix == 'ulan':
        values_clause = f'VALUES ?ulan {{{values}}}'
        query_section = """
            ?wd wdt:P245 ?ulan.
        """
    elif prefix == 'viaf':
        values_clause = f'VALUES ?viaf {{{values}}}'
        query_section = """
            ?wd wdt:P214 ?viaf.
        """
    elif prefix == 'wd':
        select_clause = f"""
        SELECT ?gnd ?ulan ?viaf ?wd WHERE {{
        """
        values_clause = f'VALUES ?wd {{{values}}}'
        query_section = """
            OPTIONAL { ?wd wdt:P227 ?gnd. }
            OPTIONAL { ?wd wdt:P245 ?ulan. }
            OPTIONAL { ?wd wdt:P214 ?viaf. }
        """
    else:
        raise NotImplementedError(f"This prefix is not implemented: {prefix}")

    # Complete query
    query = f"{select_clause} {values_clause} {query_section} }}"

    return query


def execute_sparql_query(endpoint, query):
    '''
    Executes a SPARQL query on the specified endpoint and returns the results.
    '''
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-agent", USER_AGENT)
    try:
        results = sparql.query().convert()
        return results

    except Exception as e:
        print(f"SPARQL query failed: {e}")
        return None


def process_authority(prefix, values, WD_SPARQL_ENDPOINT):
    '''
    Builds a SPARQL query based on the provided prefix and values,
    then executes the query on the specified Wikidata SPARQL endpoint.
    Returns the query result.
    '''
    query = build_sparql_query(prefix, values)
    if query is None:
        print("The query was not generated.")
        return None

    return execute_sparql_query(WD_SPARQL_ENDPOINT, query)