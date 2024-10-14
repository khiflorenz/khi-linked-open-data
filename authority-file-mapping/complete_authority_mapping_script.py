#!/usr/bin/env python
# coding: utf-8


import csv
import os
import xml.etree.ElementTree as ET
import argparse
import pandas as pd
import datetime
import re
from collections import defaultdict
from SPARQLWrapper import SPARQLWrapper, JSON, CSV, XML
import requests
import numpy as np  



ULAN_SPARQL_ENDPOINT = "http://vocab.getty.edu/sparql"
VIAF_SPARQL_ENDPOINT = "https://viaf.org/viaf/data/"
WD_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

prefixes_dict = {"gnd": "gnd", "ulan": "ulan", "viaf":"viaf"}
USER_AGENT = "mapping_khi_authority_data/1.0 (alessandra.failla@khi.fi.it) Python/3.10"



#STEP 1
def extract_a30gn(xml_content):
    '''
    Extracts content from the xml <a30gn> element, which includes authority data, and returns it.
    '''
    # Define the default namespace mapping
    namespaces = {'default': 'http://www.openarchives.org/OAI/2.0/'}

    # Parse the XML content
    root = ET.fromstring(xml_content)

    # Find the <a30gn> element within the default namespace (identifies artist identifier)
    a30gn = root.find('.//default:a30gn', namespaces)

    # If the element exists, return its text content
    if a30gn is not None:
        return a30gn.text
    else:
        return None
    

def extract_authority_data(folder_path):
    '''
    Extracts authority data from .xml files and stores it into a text file.
    Each line contains each record followed by the related identifiers separated by a comma.
    Returns the text file name.
    '''
    # Generate default output file names
    #timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_initial_extraction = f"khi_a30gn_data.txt"

    # Open the output file in append mode, count of total documents processed and times content was extracted
    with open(output_initial_extraction, 'a', newline='') as f_out:
        total_documents = 0
        extracted_count = 0

        # Iterate over all XML files in the folder
        for file_name in os.listdir(folder_path):
            if re.match(r'^oai_kue_0*7', file_name) and file_name.endswith('.xml'):
                file_path = os.path.join(folder_path, file_name)
                try:
                    # Open and read the content of the XML file and extract the <a30gn> content related to authority data
                    with open(file_path, 'r', encoding='utf-8') as f_in:
                        a30gn_content = extract_a30gn(f_in.read())

                        # If the content is found, write it to the verbose and shorter output files
                        if a30gn_content:
                            a30gn_content = a30gn_content.replace("; ", ", ")
                            f_out.write(f"{file_name},{a30gn_content}\n")
                            extracted_count += 1  # Increment count of extracted content

                except Exception as e:
                    print(f"Error processing file {file_name}: {e}")

                total_documents += 1
                if total_documents % 500 == 0:
                    print(f"Documents inspected: {total_documents}, Content extracted: {extracted_count}")

        # Print final counts after processing all documents
        print(f"Total documents inspected: {total_documents}, Total content extracted: {extracted_count}")
    return output_initial_extraction


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


# STEP 2
def process_txt_to_pd(input_file):
    '''
    Takes a file name as input and converts it into DataFrame format. Each identifier in each line
    is divided into its prefix and following ID; prefixes are used to name the DataFrame's columns.
    '''
    # Dictionary to hold data by key_khi
    data = {}
    unique_prefixes = []
    unmatched_values = []

    # Read the input file
    with open(input_file, 'r') as f:
        for line in f:
            # Strip whitespace and skip empty lines
            line = line.strip()
            if not line:
                continue
            
            # Split the line into key and values
            parts = [part.strip() for part in line.split(',')]
            key_khi = parts[0]  # The first element is the key_khi
            values = parts[1:]  # The rest are the values
            
            # Initialize the entry for this key_khi
            if key_khi not in data:
                data[key_khi] = {}

            # Process each value to extract prefix and number
            for value in values:
                if value:
                    match = re.match(r"([a-zA-Z]+)(\d+)", value)
                    if match:
                        prefix = match.group(1).lower()
                        # Store the value under the correct prefix
                        data[key_khi][prefix] = match.group(2)
                        if prefix not in unique_prefixes:
                            unique_prefixes.append(prefix)
                    else:
                        unmatched_values.append(value)

    # Create a DataFrame from the dictionary
    df = pd.DataFrame.from_dict(data, orient="index")
    df["key_khi"] = df.index
    df.reset_index(inplace=True, drop = True)
    
    # Reorder columns so "key_khi" is the first column
    cols = df.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]

    for prefix in prefixes_dict.values():
        if prefix not in df.columns:
            df[prefix] = pd.NA

    # Filter rows where "key_khi" matches patterns identifying KHI-records and drop empty columns
    df_khi = df.loc[df['key_khi'].str.match(r'^oai_kue_0*7')]
    df_khi.reset_index(drop=True, inplace=True)

    if unmatched_values:
        with open('unmatched_authority_data.txt', 'a') as log_file:
            for unmatched in unmatched_values:
                log_file.write(f"{unmatched}\n")
    
    return df_khi


def mapping_from_wikidata(output_df):
    '''
    Takes a dataframe as input containing originally available identifiers and their Wikidata mapping
    (if available). Uses Wikidata entities to retrieve missing ULANs, GNDs, VIAFs, if available on Wikidata.
    '''
    tmp_df = output_df['wd'].dropna()
    batch_size = 100
    num_batches = (len(tmp_df) + batch_size -1) // batch_size
    print(f"now executing wd")
    for batch_index in range(num_batches):
            
            # batch = tmp_df.iloc[0:200]...[200:400]...
            batch = tmp_df.iloc[batch_index * batch_size:(batch_index+1) * batch_size]
        
            batch_values = batch.apply(lambda x: f'<{x}>').astype(str)
            col_to_string = ' '.join(batch_values)

            print(f"Processing batch {batch_index + 1}/{num_batches}")
            query_result = process_authority('wd', col_to_string, WD_SPARQL_ENDPOINT)

            if query_result:
                # Iterate over the batch to match results with the original DataFrame
                for index, value in batch.items():
                    
                    for prefix in prefixes_dict:
                        matched_prefix_values = []
                        for binding in query_result['results']['bindings']:
                    
                            if binding['wd']['value'] == value and prefix in binding.keys():
                                if binding[prefix]['value'] not in matched_prefix_values:
                                    matched_prefix_values.append(binding[prefix]['value'])

                        if len(matched_prefix_values) > 0:
                            current_prefix_value = output_df.at[index, prefix]
                            # Handle current_prefix_value safely
                            if pd.isna(current_prefix_value):
                                current_prefix_value = ""
                            else:
                                current_prefix_value = str(current_prefix_value)

                            total_matched = "; ".join(matched_prefix_values)
                            if current_prefix_value == "":
                                output_df.at[index, prefix] = total_matched
                            elif total_matched != current_prefix_value:
                                output_df.at[index, prefix] = current_prefix_value + "; " + total_matched

    output_df['wd'] = output_df['wd'].apply(lambda x: f"wd:{x.split('/')[-1]}" if x != "" else "")
    return output_df
    


def process_and_map_data(folder_path, WD_SPARQL_ENDPOINT):
    '''
    Converts input text file into a DataFrame through the auxiliary function.
    Isolated each column and create batches to extract values for the query avoiding errors.
    Performs a query for batches in each column (gnd -> wd, ulan -> wd, viaf -> wd)

    '''
    output_initial_extraction=extract_authority_data(folder_path)
    ordered_csv_output = f"ordered_{output_initial_extraction[:-4]}.csv"
    # Convert authority data into ordered csv file with columns sorted by authority file
    authority_df = process_txt_to_pd(output_initial_extraction)
    output_df = authority_df.copy()

    # Initialize the new column to store query results
    output_df["wd"] = ""

    for col in authority_df.columns:
        if col == "key_khi" or col not in prefixes_dict.values():
            continue
        print(f"now executing {col}")
        prefix = prefixes_dict[col]

        #Create temporary single-column DataFrame to prepare data in each column for query; remove empty cells.
        tmp_df = authority_df[col].dropna()

        batch_size = 200
        num_batches = (len(tmp_df) + batch_size -1) // batch_size

        for batch_index in range(num_batches):
            # batch = tmp_df.iloc[0:200]...[200:400]...
            batch = tmp_df.iloc[batch_index * batch_size:(batch_index+1) * batch_size]

            batch_values = batch.apply(lambda x: f'"{x}"').astype(str)
            col_to_string = ' '.join(batch_values)

            print(f"Processing batch {batch_index + 1}/{num_batches}")

            query_result = process_authority(prefix, col_to_string, WD_SPARQL_ENDPOINT)

            if query_result:
                # Iterate over the batch to match results with the original DataFrame
                for index, value in batch.items():
                    # Initialize a list to hold additional matches
                    matched_wd_values = []

                    for binding in query_result['results']['bindings']:
                        # Match the value with the query result
                        if binding[prefix]['value'] == value:
                            matched_wd_values.append(binding['wd']['value'])

                    # Update the output DataFrame
                    if matched_wd_values:
                        current_wd_value = output_df.at[index, "wd"]

                        if current_wd_value == "":
                            # Join matched values for "wd"
                            output_df.at[index, "wd"] = matched_wd_values[0]

                        elif output_df.at[index, 'wd'] != matched_wd_values[0]:
                            conflict_message = f"Wikidata conflict: {output_df.at[index, 'wd']},{matched_wd_values[0]}"
                            print(conflict_message)
                            with open('wd_conflicts_log.txt', 'a') as log_file:
                                log_file.write(conflict_message + '\n')

    # Complete data with reverse mapping from wikidata
    output_df = mapping_from_wikidata(output_df)

    # Remove empty columns
    output_df.dropna(axis=1, how='all', inplace=True)
    authority_df.dropna(axis=1, how='all', inplace=True)

    for col in output_df:
        if col in prefixes_dict.values():
            output_df[col] = output_df[col].apply(lambda x: "; ".join([f"{col}:{val.strip()}" for val in x.split("; ")]) if pd.notna(x) else x)


    # Save the output DataFrame to CSV
    output_df.to_csv(ordered_csv_output, index=False)
    print(f"Results saved to {ordered_csv_output}")
    return output_df, ordered_csv_output
        

def extract_map_replace_xml(folder_path):
    '''
    Calls previous function to create a DataFrame with authority file data mappings.
    Iterate over the XML in the specified folder to find matches with file names in the DataFrame and replaces the content
    of <a30gn> with the corresponding DataFrame row, joining its content with ; as separator.
    '''
    mapping_dataframe, mapping_csv = process_and_map_data(folder_path, WD_SPARQL_ENDPOINT)

    namespaces = {'default': 'http://www.openarchives.org/OAI/2.0/'}

    for index, row in mapping_dataframe.iterrows():
        file_name = row['key_khi']  # Assuming key_khi holds the file name
        
        file_path = os.path.join(folder_path, file_name)

        if os.path.exists(file_path):
            joined_values = "; ".join(str(value) for value in row[1:] if pd.notna(value))
            
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()

                a30gn_element = root.find('.//default:a30gn', namespaces)

                if a30gn_element is not None:
                    a30gn_element.text = joined_values
                    
                    tree.write(file_path, encoding='utf-8', xml_declaration=True)
                    print(f"Replaced content in file {file_name}")
                else:
                    print(f"<a30gn> element not found in {file_name}")
                    
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
        else:
            print(f"File {file_name} not found in the folder.")

    print("Process completed.")
    return mapping_dataframe, mapping_csv


if __name__ == '__main__':
    # Create an argument parser
    parser = argparse.ArgumentParser(
        description="Script to extract, map, and replace XML content based on authority data")

    # Add a positional argument for the folder_path
    parser.add_argument('folder_path', type=str, help='Path to the folder containing XML files')

    # Parse the arguments
    args = parser.parse_args()

    # Call the main function to extract, map, and replace XML content
    mapping_result, mapping_result_csv = extract_map_replace_xml(args.folder_path)


