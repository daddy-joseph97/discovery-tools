"""Utilities to bootstrap functionality, data, mock deployment, and other useful
'thingies'.

"""
import json
import sys
import os
import os.path

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

def load_users(mongo_uri=None, users_file=None, roles_file=None, overwrite=True,
               clean_collections=True):
    """Loads users (and optionally roles) into the specified database. Note that
    each JSON data's list much contain unique entries as per the respective type.
    For roles, the ``role`` key must be unique and for users the ``username`` key
    must be unique.  Instead of validating this, merely keep first entry of a given
    key will be used only and all other entries disregarded.  Example:
        roles = [{'role': 'The Larch',
                  'description': 'Our first entry is... The Larch'},
                  {'role': 'The Larch',
                  'description': 'and now for something completely different'}]
    
    In this case, only the first role with the description of 'Our first entry
    is... The Larch' will be entered.
    
    Args:
        mongo_uri (str): URL to connect to the mongodb instance.
        users_file (str): Path to JSON file containing users to load.  'username'
            is unique. (email is also but not enforced currently)
        roles_file (str): Optional path to JSON file containing roles users have.
            'role' is unique.
        overwrite (bool): Default True; if True, any existing user or role entry
            is overwritten by entry in respective JSON files.
        clean_collections (bool): Default True; if True, cleans users and roles collections
            before writing entries from respective JSON files.
    
    """
    if users_file is None:
        msg = 'Required parameter \'users_file\' missing.'
        print(msg)
        sys.exit(1)        
    mongo_uri = mongo_uri or 'mongodb://127.0.0.1:27017/wxdata'
    db_url = urlparse(mongo_uri)
    conn = pymongo.Connection(host=db_url.hostname, port=db_url.port)
    db = conn[db_url.path[1:]]
    if db_url.username and db_url.password:
        db.authenticate(db_url.username, db_url.password)
    
    # clean_collections, True by default, ensures we have a pristine collection(s)
    if clean_collections:
        db.drop_collection('user_roles')
        db.drop_collection('users')
    
    if roles_file is not None:
        roles_file = os.path.abspath(os.path.join(os.getcwd(), roles_file))
        try:
            in_file = open(roles_file, 'r')
        except IOError:
            msg = 'roles_file \'{}\' does not exist or cannot be read.'
            print(msg.format(roles_file))
            sys.exit(1)
        
        try:
            roles_data = json.load(in_file)
        except ValueError:
            msg = 'roles_file \'{}\' is not valid JSON and cannot be parsed as such.'
            print(msg.format(roles_file))
            sys.exit(1)
            
        # perform our highly sophisticated algorithm for dealing with conflicting
        # unique keys... just take the first and dump the dupes (take THAT Skynet)
        roles_data = list_of_seq_unique_by_key(roles_data, 'role')
        
        roles_collection = db.user_roles
        
        # bulk insert, order is maintained since this is a list input
        try:
            object_ids = roles_collection.insert(roles_data)
        except OperationFailure:
            msg = 'Bulk insert of data from roles_file \'{}\' failed; ' \
                'roles_data: {}'
            print(msg.format(roles_file, roles_data))
            sys.exit(1)
            
        # may not need this, and just query the db regardless, however this could
        # be useful to ensure what did and didn't write
        roles_map = {roles_data[idx]['role']:str(x) for idx, x in enumerate(object_ids)}
    else:
        roles_map = {}
        
    try:
        in_file = open(users_file, 'r')
    except IOError:
        msg = 'users_file \'{}\' does not exist or cannot be read.'
        print(msg.format(users_file))
        sys.exit(1)
    
    try:
        users_data = json.load(in_file)
    except ValueError:
        msg = 'users_file \'{}\' is not valid JSON and cannot be parsed as such.'
        print(msg.format(users_file))
        sys.exit(1)
        
    # deal with dupes if present
    users_data = list_of_seq_unique_by_key(users_data, 'username')
    
    users_collection = db.users
    missing_roles_map = {}
    
    # massage data
    for user in users_data:
        password = user.pop('password')
        role_names = user.pop('roles')
        
        # calculate password hash and store only that
        user['password_hash'] = hash_password(password)
        
        # we will store references to roles from the user_roles collection
        # first, ensure all roles have appropriate entry in the db
        # just in case we set clean or overwrite to False, query the db not the
        # rules_map and furthermore be detailed since this is demo and small amt
        # of data regardless
        missing_roles = []
        role_objs = []
        for role_name in role_names:
            role_obj = roles_collection.find_one({'role': role_name})
            if role_obj is not None:
                role_fields = ['role', 'descriptions']
                role_objs.append({k:v for k,v in role_obj.items() if k in role_fields})
            else:
                # to be kind, we are not stopping on first error to report all
                missing_roles.append(role_name)
        if not missing_roles and not any(missing_roles_map):
            user['roles'] = role_objs
        elif missing_roles:
            missing_roles_map[user['username']] = missing_roles
            
        # enough kindness... not validating writing to db if any missing roles
    
    if not missing_roles and not any(missing_roles_map):
        try:
            object_ids = users_collection.insert(users_data)
        except OperationFailure:
            msg = 'Bulk insert of data from users_file \'{}\' failed; ' \
                'users_data: {}'
            print(msg.format(users_file, users_data))
            sys.exit(1)
        else:
            print('Successfully wrote user data.')
            sys.exit()
    else:
        msg = 'User referenced roles are missing from user_roles collection: {}'
        print(msg.format(missing_roles_map))
        print('Note: prior writes from this operation are not being rolled back!')
        sys.exit(1)
        
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
    parser.add_argument('-u', '--usersfile', metavar='FILE',
                        help='Path to JSON file with user data.',
                        default=os.path.abspath(os.path.join(base_path, '../tests/data/users.json')))
    parser.add_argument('-r', '--rolesfile', metavar='FILE',
                        help='Path to JSON file with user roles data.')
    parser.add_argument('-p', '--preserve-collections', action='store_false', dest='clean_collections',
                        help='If specified, preserve existing collections if present.')
    parser.add_argument('-x', '--exclude_overwrites', action='store_false', dest='overwrite',
                        help='If specified, ignore existing documents in datastore.')
    args = parser.parse_args()
    
    load_users(mongo_uri=args.db_uri, users_file=args.usersfile,
               roles_file=args.rolesfile, overwrite=args.overwrite,
               clean_collections=args.clean_collections)