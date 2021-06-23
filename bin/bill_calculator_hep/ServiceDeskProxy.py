#!/usr/bin/env python
"""
Python Proxy for communication with Fermilab's Service Now implementation
using the json interface.

Requirements:
    - in the environment, set the environmental variable SERVICE_NOW_URL to
      the base url for the service desk; if this is not set, the default
      development SNOW site will be used.

"""
import sys
import traceback
import os
import urllib
import base64
import json
from urllib.request import urlopen
import getpass, http.client, json, logging, optparse, pprint, requests, sys, yaml

# constants; we expose these here so that customers have access:
NUMBER   = 'number'
SYS_ID   = 'sys_id'
VIEW_URL = 'view_url'
ITIL_STATE = 'u_itil_state'

class ServiceDeskProxy(object):
    """
    Proxy object for dealing with the service desk.
    """
    # actions:
    ACTION_CREATE_URL = 'incident.do?JSON&sysparm_action=insert'
    ACTION_UPDATE_URL = 'incident.do?JSON&sysparm_action=update&sysparm_query=sys_id='
    ACTION_VIEW_URL   = 'nav_to.do?uri=incident.do%3Fsys_id='

    class ServiceDeskProxyException(Exception): pass
    class ServiceDeskNotAvailable(ServiceDeskProxyException): pass
    class ServiceDeskInvalidResponse(ServiceDeskProxyException): pass
    
    def __init__(self, base_url, username, password):
        # the base url that will be used for contacting the service desk
        self.base_url = base_url
        
        # the username/password that will be used for contacting the service desk:
        self.username = username
        self.password = password

    #-------------------------------------------------------------------------------------
    def _get_authheader(self, username, password):
        auth = (username, password)
        return auth
    #-------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------
    def createServiceDeskTicket(self, args):
        """
        Open a service desk ticket, passing in the data specified by the kwargs.
        """
        the_url = "%s/api/now/v1/table/incident" % (self.base_url)
        print(the_url)
        return self._process_request(the_url, args)
    #-------------------------------------------------------------------------------------
    def updateServiceDeskTicket(self, sys_id=None, comments=None, **kwargs):
        """
        Update an existing service desk ticket, identified by sys_id,
        passing in "Additional Information" using the "comments" keyword, and any other
        data specified by kwargs.
        """
        the_url = self.base_url + self.ACTION_UPDATE_URL + sys_id
        return self._process_request(the_url, sys_id=sys_id, comments=comments, **kwargs)
    #-------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------
    def _process_request(self, the_url, args):
        
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        print(self.username)
        print(self.password)
        # jsonify the data passed in by the caller:
        data = json.dumps(args, sort_keys=True, indent=4)
        print(data)

        response = requests.post(the_url, auth=(self.username, self.password), headers=headers, json=args)
        print(response.json())
        try:
          j = response.json()
          incident = j['result']['number']
          return incident
        except Exception as e:
          print("error: could not create request - %s" % e)
          sys.exit(-1)
