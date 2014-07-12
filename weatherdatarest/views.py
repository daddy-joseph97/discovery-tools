""" Weather data from NOAA NCDC to test against and play in the sandbox.
"""
from pyramid.exceptions import Forbidden
from cornice import Service


api_prefix = '/api/v0.1/'
wx_hourly = Service(name='Hourly Observations',
                    path='/{}/{}'.format(api_prefix, 'hourly').replace('//', '/'),
                    description="NOAA NCDC Data - Hourly Observations")
wx_precip = Service(name='Hourly Precipitation',
                    path='/{}/{}'.format(api_prefix, 'precip').replace('//', '/'),
                    description="NOAA NCDC Data - Hourly Precipitation")
users = Service(name='Service Users',
                path='/{}/{}'.format(api_prefix, 'users').replace('//', '/'),
                description="API users")

#
# Helpers
#  
#def _create_token():
    #return binascii.b2a_hex(os.urandom(20))


#class _401(exc.HTTPError):
    #def __init__(self, msg='Unauthorized'):
        #body = {'status': 401, 'message': msg}
        #Response.__init__(self, json.dumps(body))
        #self.status = 401
        #self.content_type = 'application/json'


#def valid_token(request):
    #header = 'X-Messaging-Token'
    #token = request.headers.get(header)
    #if token is None:
        #raise _401()

    #token = token.split('-')
    #if len(token) != 2:
        #raise _401()

    #user, token = token

    #valid = user in _USERS and _USERS[user] == token
    #if not valid:
        #raise _401()

    #request.validated['user'] = user


def unique(request):
    name = request.body
    if name in _USERS:
        request.errors.add('url', 'name', 'This user exists!')
    else:
        user = {'name': name, 'token': _create_token()}
        request.validated['user'] = user


#
# Services
#

#
# User Management
#
@users.get()
def get_users(request):
    """Returns a list of all users."""
    users_cur = request.db.users.find(fields={'_id': False,
                                              'password_hash': False})
    
    return {'users': [x for x in users_cur]}

#@users.post(validators=unique)
#def create_user(request):
    #"""Adds a new user."""
    #user = request.validated['user']
    #_USERS[user['name']] = user['token']
    #return {'token': '%s-%s' % (user['name'], user['token'])}

#@users.delete(validators=valid_token)
#def del_user(request):
    #"""Removes the user."""
    #name = request.validated['user']
    #del _USERS[name]
    #return {'Goodbye': name}
    
#
# Hourly weather observations
#
@wx_hourly.get()
def get_records(request):
    """Return a list of hourly weather records (observations). Limit to 10 for now.
    
    """
    records_cur = request.db.hourly.find(fields={'_id': False,},
                                         limit=10)
        
    return {'records': [x for x in records_cur]}

#
# Hourly precipitation
#
@wx_precip.get()
def get_records(request):
    """Return a list of hourly precipitation records. Limit to 10 for now.
    
    """
    records_cur = request.db.precip.find(fields={'_id': False,},
                                         limit=10)
    
    return {'records': [x for x in records_cur]}