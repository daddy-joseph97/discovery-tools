""" Weather data from NOAA NCDC to test against and play in the sandbox.
"""
from pyramid.exceptions import Forbidden
from cornice import Service
from cornice.resource import resource, view
from webob import exc


api_prefix = '/api/v0.1/'
#wx_hourly = Service(name='Hourly Observations',
                    #path='/{}/{}'.format(api_prefix, 'hourlyrecordings').replace('//', '/'),
                    #description="NOAA NCDC Data - Hourly Observations")
#wx_precip = Service(name='Hourly Precipitation',
                    #path='/{}/{}'.format(api_prefix, 'preciprecordings').replace('//', '/'),
                    #description="NOAA NCDC Data - Hourly Precipitation")
#users = Service(name='Service Users',
                #path='/{}/{}'.format(api_prefix, 'users').replace('//', '/'),
                #description="API users")

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
#@resource(collection_path='/{}/{}'.format(api_prefix, 'users').replace('//', '/'),
          #path='/{}/{}'.format(api_prefix, '/users/{id}').replace('//', '/'))
@resource(collection_path='/{}/{}'.format(api_prefix, 'users').replace('//', '/'),
          path='/api/v0.1/users/{username}')         
class User(object):
    def __init__(self, request):
        self.request = request
        
    def collection_get(self):
        """Returns a list of all users.
        
        """
        users_cur = self.request.db.users.find(fields={'_id': False,
                                                       'password_hash': False})
        
        return {'users': [x for x in users_cur]}

    @view(renderer='json')
    def get(self):
        """Return single user by username.  Note that it will be returned as a
        single resource (not list) and not as the value of 'users' as with the
        collection GET.
        
        """
        username = self.request.matchdict['username']
        user = self.request.db.users.find_one({'username': username},
                                              fields={'_id': False,
                                                      'password_hash': False}                                              )
        
        if user is None:
            msg = 'User with username \'{}\' not found.'
            raise exc.HTTPNotFound(msg.format(username))
        
        return user
    

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
@resource(collection_path='/{}/{}'.format(api_prefix, 'hourlyrecordings').replace('//', '/'),
          path='/api/v0.1/hourlyrecordings/{wban}')
class HourlyWxReadings(object):
    def __init__(self, request):
        self.request = request
        
    def collection_get(self):
        """Returns a list of all hourly recordings.
        
        .. note: currently defaulted with limit of 5 and unsorted
        
        """
        limit = int(self.request.GET.get('limit', 5))
        wban = self.request.GET.get('wban', None)
        recordings_cur = self.request.db.hourly.find({'WBAN': wban},
                                                     fields={'_id': False},
                                                     limit=limit)
        
        return {'hourly_recordings': [x for x in recordings_cur]}

    def get(self):
        """Return latest recording at WBAN.  Note that it will be returned as a
        single resource (not list) and not as the value of 'hourly_recordings' as
        with the collection GET.
        
        .. note: currenly returning any (pymongo find_one) result with supplied
            WBAN
        
        """
        wban = self.request.matchdict['wban']
        recording = self.request.db.hourly.find_one({'WBAN': wban},
                                                    fields={'_id': False,})
        
        if recording is None:
            msg = 'Reading from WBAN station \'{}\' not found.'
            raise exc.HTTPNotFound(msg.format(wban))
        
        return recording

#
# Hourly precipitation
#
@resource(collection_path='/{}/{}'.format(api_prefix, 'preciprecordings').replace('//', '/'),
          path='/api/v0.1/preciprecordings/{wban}')
class HourlyPrecipReadings(object):
    def __init__(self, request):
        self.request = request
        
    def collection_get(self):
        """Returns a list of all hourly precipitation recordings.
        
        .. note: currently defaulted with limit of 5 and unsorted
        
        """
        limit = int(self.request.GET.get('limit', 5))
        wban = self.request.GET.get('wban', None)
        recordings_cur = self.request.db.precip.find({'WBAN': wban},
                                                     fields={'_id': False},
                                                     limit=limit)
        
        return {'precip_recordings': [x for x in recordings_cur]}

    def get(self):
        """Return latest precipitation recording at WBAN.  Note that it will be
        returned as a single resource (not list) and not as the value of
        'precip_recordings' as with the collection GET.
        
        .. note: currenly returning any (pymongo find_one) result with supplied
            WBAN
        
        """
        self.request.matchdict['wban']
        recording = self.request.db.precip.find_one({'WBAN': wban},
                                                    fields={'_id': False,})
        
        if recording is None:
            msg = 'Reading from WBAN station \'{}\' not found.'
            raise exc.HTTPNotFound(msg.format(wban))
        
        return recording