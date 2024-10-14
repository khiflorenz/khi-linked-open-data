#!/usr/bin/env python3
# coding: utf-8
from sickle import Sickle
#from sickle.iterator import OAIResponseIterator
from datetime import datetime, timedelta
import os
import re



def complete_datetime(date_str):
    """
    Completes a date string to match the OAI-PMH format 'YYYY-MM-DDThh:mm:ssZ'.
    If the date string is already in the correct format, it's returned as-is.
    If it's missing time, day, month, or timezone, these are automatically assumed.
    Please note that all times are going to be in .... format.

    Supported formats:
    - 'YYYY'
    - 'YYYY-MM'
    - 'YYYY-MM-DD'
    - 'YYYY-MM-DDThh:mm'
    - 'YYYY-MM-DDThh:mm:ss'
    - 'YYYY-MM-DDThh:mm:ssZ' (correct format)
    """
    if isinstance(date_str, datetime):
        # Convert datetime object to string in ISO format
        date_str = date_str.strftime('%Y-%m-%dT%H:%M:%S%z')  # No 'Z' for timezone-aware

    if not isinstance(date_str, str):
        raise TypeError("Input must be a string or datetime object.")

    # Strip any leading/trailing whitespace
    date_str = date_str.strip()

    # Define formats for parsing
    formats = [
        '%Y-%m-%dT%H:%M:%SZ',   # Full format
        '%Y-%m-%dT%H:%M:%S',    # Missing 'Z'
        '%Y-%m-%dT%H:%M',       # Missing seconds and 'Z'
        '%Y-%m-%d',             # Missing time
        '%Y-%m',                # Missing day and time
        '%Y'                    # Missing month, day, and time
    ]

    # Try each format to parse the date string
    for fmt in formats:
        try:
            # Parse the date string
            dt = datetime.strptime(date_str, fmt)
            # Return the completed date string based on the format
            if fmt == '%Y-%m-%dT%H:%M:%SZ':
                return date_str  # Already in the correct format
            elif fmt == '%Y-%m-%dT%H:%M:%S':
                return f"{date_str}Z"  # Append 'Z'
            elif fmt == '%Y-%m-%dT%H:%M':
                return f"{date_str}:00Z"  # Append seconds and 'Z'
            elif fmt == '%Y-%m-%d':
                return f"{date_str}T00:00:00Z"  # Append time and 'Z'
            elif fmt == '%Y-%m':
                return f"{date_str}-01T00:00:00Z"  # Append day, time, and 'Z'
            elif fmt == '%Y':
                return f"{date_str}-01-01T00:00:00Z"  # Append month, day, time, and 'Z'
        except ValueError:
            continue

    # If no format worked, raise an exception
    raise ValueError(f"Date '{date_str}' is not in a valid format.")



def select_directory(oai_identifier, base_output_dir, category_mapping):
    # Iterate through the dictionary to check if any key is in the identifier
    if category_mapping is not None:
        for key, category in category_mapping.items():
            if key in oai_identifier.lower():
                # If the key is found, create the corresponding directory
                output_dir = os.path.join(base_output_dir, category)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                return output_dir
    # If no key is found, assign the record to 'uncategorized'
    output_dir = os.path.join(base_output_dir, 'uncategorized')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir



def read_last_date_from_file(txtpath):
    # Reads the last date entry from the specified file or returns a default date.
    try:
        with open(txtpath, 'r') as file:
            lines = file.readlines()
        if lines and re.match(r'^\d{4}', lines[-1].strip()):
            return complete_datetime(lines[-1].strip())
        return "2015-01-01T00:00:00Z"
    except FileNotFoundError:
        raise FileNotFoundError(f"File at '{txtpath}' not found. Please provide a valid path.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file: {e}")



def append_current_date_to_file(txtpath, date_to_write = None):
    # Appends the current date to the specified file.
    if date_to_write is None:
        date_to_write = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(txtpath, 'a') as file:
        file.write('\n' + date_to_write)
    return date_to_write




def handle_dates(txtpath=None):
    if txtpath is None:
        txtpath = 'harvest_date.log'
    if os.path.exists(txtpath):
        fromdate = read_last_date_from_file(txtpath)
    else:
        fromdate = "2015-01-01T00:00:00Z"
    return fromdate, txtpath



def save_record(response, output_dir, oai_identifier):
    # Remove the unwanted part: '30gn= gnd...' or ' 30gn= gnd...'
    clean_identifier = re.sub(r'\s+.*30gn= [^\s]+', '', oai_identifier)

    # Replace '/' and '::' with '_'
    safe_oai_identifier = clean_identifier.replace('/', '_').replace('::', '_')

    # Create the file path
    file_path = os.path.join(output_dir, f'{safe_oai_identifier}.khi.xml')

    # Save the response to the file
    with open(file_path, 'wb') as fp:
        fp.write(response.raw.encode('utf8'))

    print(f"Saved response to '{file_path}'")



def harvest_timespan(provider,
                     metadataprefix=None,
                     txtpath=None,
                     fromdate=None,
                     untildate=None,
                     oaiset=None,
                     record_type_dict=None,
                     base_output_dir = None,
                    ):
    # Ensure that provider is specified
    if provider is None:
        raise ValueError("Please specify a data provider.")
    if base_output_dir is None:
        base_output_dir = 'dataset_xml'

    # Initialize Sickle
    sickle = Sickle(provider) #, iterator=OAIResponseIterator

    # Handle the date logic
    if fromdate is None:
        fromdate, txtpath = handle_dates(txtpath)

    # Ensure 'fromdate' and 'untildate' are properly formatted
    fromdate_completed = complete_datetime(fromdate)
    untildate = complete_datetime(untildate) if untildate else datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Retrieve records from the provider based on the specified parameters
    responses = sickle.ListRecords(**{'metadataPrefix': metadataprefix, 'from': fromdate_completed, 'until': untildate, 'set': oaiset})

    # Set the limit for downloads
    response_count = 0
    #max_downloads = 20 # For testing


    while True:
        # For testing
        #if response_count >= max_downloads:
        #    print(f"Download limit reached: {max_downloads} records.")
        #    break
        try:
            response = responses.next()
            oai_identifier = response.header.identifier
            if not oai_identifier:
                print(f"Identifier not found in the response. Skipping...")
                continue

            # Get the category and output directory
            output_dir = select_directory(oai_identifier, base_output_dir, record_type_dict)

            # Save the response
            save_record(response, output_dir, oai_identifier)

            response_count += 1

        except StopIteration:
            break

    print(f"Total {response_count} records saved.")
    append_current_date_to_file(txtpath, untildate)
    return response_count



def harvest_timespan_safe(provider,
                          metadataprefix=None,
                          txtpath=None,
                          untildate=None,
                          oaiset=None,
                          record_type_dict=None):
    # Ensure that provider is specified
    if provider is None:
        raise ValueError("Please specify a data provider.")

    # Handle the date logic
    fromdate, txtpath = handle_dates(txtpath)
    fromdate = complete_datetime(fromdate)
    untildate = complete_datetime(untildate) if untildate else datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Convert fromdate and untildate to datetime objects for iteration
    fromdate_dt = datetime.strptime(fromdate, '%Y-%m-%dT%H:%M:%SZ')
    untildate_dt = datetime.strptime(untildate, '%Y-%m-%dT%H:%M:%SZ')

    response_count = 0
    #max_downloads = 20  # For testing

    current_date = fromdate_dt

    while current_date <= untildate_dt:
        next_date = current_date + timedelta(days=1)
        date_str = current_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        next_date_str = next_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Debugging prints
        #print(f"Requesting records from {date_str} to {next_date_str}")

        # Call harvest_timespan with the current date range
        try:
            count = harvest_timespan(
                provider=provider,
                metadataprefix=metadataprefix,
                txtpath=txtpath,
                fromdate=date_str,
                untildate=next_date_str,
                oaiset=oaiset,
                record_type_dict=record_type_dict
            )
        except Exception as e:
            count = 0  # Handle or log error as needed

        response_count += count

        if count > 0:
            print(f"Found {count} records for {date_str}")
        #else:
        #    print(f"No records found for {date_str}")

        # For testing:
        #if response_count >= max_downloads:
        #    print(f"Download limit reached: {max_downloads} records.")
        #    return response_count

        current_date = next_date

    print(f"Total {response_count} records saved.")
    return response_count



provider_khi = "https://aps-production.khi.fi.it/oai-pmh"
oai_set = "website"
metadata_prefix = "khi"
category_mapping = {
    '::kue::': 'artist',
    '::obj::': 'artwork',
    '::lit::': 'literature',
    '::oak::': 'online_exhibition',
    '::oau::': 'exhibition_presentation'
}



# Call with txt path
#records_khi=harvest_timespan(provider=provider_khi, metadataprefix=metadata_prefix, oaiset=oai_set,record_type_dict=category_mapping)

# Call with from-date
#records_khi=harvest_timespan(provider=provider_khi, metadataprefix=metadata_prefix, txtpath="harvest_date_log.txt", oaiset=oai_set, record_type_dict=category_mapping)

# Safe harvest - 1 day iterations to avoid timeout
records_khi_safe=harvest_timespan_safe(provider=provider_khi, metadataprefix=metadata_prefix, oaiset=oai_set, record_type_dict=category_mapping)
print(f"Total records harvested: {records_khi_safe}")





