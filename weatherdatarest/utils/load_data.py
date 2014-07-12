"""Utilities to bootstrap functionality, data, mock deployment, and other useful
'thingies'.

"""
import json
import sys
import os
import os.path
import re
import csv

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import pymongo
from pymongo.errors import OperationFailure

from weatherdatarest.utils.datamanip import hash_password


def list_of_seq_unique_by_key(seq, key):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if x[key] not in seen and not seen_add(x[key])]

def load_data(mongo_uri=None, csv_file=None, fields=None, missing_keys=None,
              missing_vals=None, overwrite=True, clean_collections=True,
              collection_name=None):
    """Loads weather (and potentially other data) into the specified database from CSV files.
    I am only testing NOAA/NWS NCDC data from `the QCLCD site`_.
    
    Args:
        mongo_uri (str): URL to connect to the mongodb instance.
        csv_file (str): Path to CSV file containing data to load.
        fields (list): Field names (matching order in CSV) if not using header in the CSV.
            If not present then field names are provided by the first row in the CSV and
            thus this also, if provided, will override that (also would ensure that the
            first row, 0, is treated as data).
        missing_keys: Used as key for sequence of key names in the event of more data in
            a given row than is in the effective field names (from `fields` or first row).
        missing_vals: Used as value in the event of less data in a row than is in the
            effective field names (from `fields` or first row).
        overwrite (bool): Default True; if True, any existing entry is overwritten by entry
            in respective file.
        clean_collections (bool): Default True; if True, cleans respective collection
            before writing entries from file.
        collection_name (str): The name of the collection to put the data in.  If not
            provided, then obtain from file name (parsing out date and extension).
            
    .. _`the QCLCD site`: http://cdo.ncdc.noaa.gov/qclcd/QCLCD?prior=N
    
    """
    if csv_file is None:
        msg = 'Required parameter \'csv_file\' missing.'
        print(msg)
        sys.exit(1)        
    mongo_uri = mongo_uri or 'mongodb://127.0.0.1:27017/wxdata'
    db_url = urlparse(mongo_uri)
    conn = pymongo.Connection(host=db_url.hostname, port=db_url.port)
    db = conn[db_url.path[1:]]
    if db_url.username and db_url.password:
        db.authenticate(db_url.username, db_url.password)
    
    if collection_name is None:
        pattern = re.compile(r'[0-9]{6}([a-zA-Z]+)\.txt')
        re_match = pattern.match(os.path.basename(csv_file))
        if not re_match:
            msg = 'Could not obtain collection name from csv filename \'{}\' and ' \
                'collection_name was not provided.'
            print(msg.format(csv_file))
            sys.exit(1)
        collection_name = re_match.group(1)
    
    # clean_collections, True by default, ensures we have a pristine collection(s)
    if clean_collections:
        db.drop_collection(collection_name)
        
    db_collection = db[collection_name]
    
    # in case you are wondering, and I know you are, why I am not using the ``with``
    # context manager, it's because I want to actually process an IOError (or other
    # error in the future) and haven't written a custom context manager yet
    try:
        in_file = open(csv_file, 'r')
    except IOError:
        msg = 'users_file \'{}\' does not exist or cannot be read.'
        print(msg.format(users_file))
        sys.exit(1)
    else:
        records = csv.DictReader(in_file)
        try:
            db_collection.insert(records)
        except OperationFailure:
            msg = 'Bulk insert of data from csv_file \'{}\' failed'
            print(msg.format(csv_file))
            sys.exit(1)
        else:
            print('Successfully wrote CSV data.')
            sys.exit()
    finally:
        in_file.close()
        
if __name__ == '__main__':
    import argparse
    import os
    import os.path
    
    base_path = os.path.dirname(sys.argv[0])
    
    parser = argparse.ArgumentParser(description='Load user data into datastore.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-d', '--datastore', metavar='DB_URI',
                        help='MongoDB URI formatted string', dest='db_uri',
                        default='mongodb://localhost:27017/wxdata')
    parser.add_argument('-f', '--csvfile', metavar='FILE',
                        help='Path to CSV file with data.')
    parser.add_argument('-n', '--fieldnames', metavar='FILE',
                        help='Comma separated list of fieldnames.')
    parser.add_argument('-k', '--missingkeys',
                        help='Key (sequenced) to be used in the event of more row data than fields.')
    parser.add_argument('-v', '--missingvals',
                        help='Value to be used in the event of less row data than fields.')
    parser.add_argument('-p', '--preserve-collections', action='store_false', dest='clean_collections',
                        help='If specified, preserve existing collections if present.')
    parser.add_argument('-x', '--exclude_overwrites', action='store_false', dest='overwrite',
                        help='If specified, ignore existing documents in datastore.')
    parser.add_argument('-c', '--collection-name',
                        help='Collection name in database, otherwise obtain from filename.')
    args = parser.parse_args()
    
    load_data(mongo_uri=args.db_uri, csv_file=args.csvfile,
               fields=args.fieldnames, missing_keys=args.missingkeys,
               missing_vals=args.missingvals,
               overwrite=args.overwrite,
               clean_collections=args.clean_collections,
               collection_name=args.collection_name)