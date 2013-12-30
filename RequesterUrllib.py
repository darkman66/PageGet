# -*- coding: utf-8 -*-
import sys, os, gzip, urllib
from pprint import pprint
from Requester import Requester
from hashlib import md5
#import urllib2
from urllib2 import  URLError, Request, urlopen
#from StringIO import StringIO
import socket


COOKIEFILE = 'cookies.lwp'          # the path and filename that you want to use to save your cookies in
import os.path


class RequesterUrllib(Requester):
    
    def __init__(self, _printing=False, _use_cache=False):
        Requester.__init__(self, _printing, _use_cache)
        
        # Table mapping response codes to messages; entries have the
        # form {code: (shortmessage, longmessage)}.
        self.responses = {
            100: ('Continue', 'Request received, please continue'),
            101: ('Switching Protocols',
                  'Switching to new protocol; obey Upgrade header'),
        
            200: ('OK', 'Request fulfilled, document follows'),
            201: ('Created', 'Document created, URL follows'),
            202: ('Accepted',
                  'Request accepted, processing continues off-line'),
            203: ('Non-Authoritative Information', 'Request fulfilled from cache'),
            204: ('No Content', 'Request fulfilled, nothing follows'),
            205: ('Reset Content', 'Clear input form for further input.'),
            206: ('Partial Content', 'Partial content follows.'),
        
            300: ('Multiple Choices',
                  'Object has several resources -- see URI list'),
            301: ('Moved Permanently', 'Object moved permanently -- see URI list'),
            302: ('Found', 'Object moved temporarily -- see URI list'),
            303: ('See Other', 'Object moved -- see Method and URL list'),
            304: ('Not Modified',
                  'Document has not changed since given time'),
            305: ('Use Proxy',
                  'You must use proxy specified in Location to access this '
                  'resource.'),
            307: ('Temporary Redirect',
                  'Object moved temporarily -- see URI list'),
        
            400: ('Bad Request',
                  'Bad request syntax or unsupported method'),
            401: ('Unauthorized',
                  'No permission -- see authorization schemes'),
            402: ('Payment Required',
                  'No payment -- see charging schemes'),
            403: ('Forbidden',
                  'Request forbidden -- authorization will not help'),
            404: ('Not Found', 'Nothing matches the given URI'),
            405: ('Method Not Allowed',
                  'Specified method is invalid for this server.'),
            406: ('Not Acceptable', 'URI not available in preferred format.'),
            407: ('Proxy Authentication Required', 'You must authenticate with '
                  'this proxy before proceeding.'),
            408: ('Request Timeout', 'Request timed out; try again later.'),
            409: ('Conflict', 'Request conflict.'),
            410: ('Gone',
                  'URI no longer exists and has been permanently removed.'),
            411: ('Length Required', 'Client must specify Content-Length.'),
            412: ('Precondition Failed', 'Precondition in headers is false.'),
            413: ('Request Entity Too Large', 'Entity is too large.'),
            414: ('Request-URI Too Long', 'URI is too long.'),
            415: ('Unsupported Media Type', 'Entity body in unsupported format.'),
            416: ('Requested Range Not Satisfiable',
                  'Cannot satisfy request range.'),
            417: ('Expectation Failed',
                  'Expect condition could not be satisfied.'),
        
            500: ('Internal Server Error', 'Server got itself in trouble'),
            501: ('Not Implemented',
                  'Server does not support this operation'),
            502: ('Bad Gateway', 'Invalid responses from another server/proxy.'),
            503: ('Service Unavailable',
                  'The server cannot process the request due to a high load'),
            504: ('Gateway Timeout',
                  'The gateway server did not receive a timely response'),
            505: ('HTTP Version Not Supported', 'Cannot fulfill request.'),
        }
        
        
    def GetRawPage(self, set_params=None, method='POST'):
        # timeout in seconds
        self.page_from_cache = False
        
        if self.use_cache == True:
            if self._devel == True:
                page = self.GetTempFile()
                if page is not None:
                    print '>> get page from cache: %s' % self.hash_name
                    return page 
                else:
                    print 'use cache but NO page in cache:', self.download_temp+self.hash_name
        try:
            if self._devel==True:
                print '[metoda] GetRawPage, obiekt URLLIB'
            timeout = 10
            socket.setdefaulttimeout(timeout)
    
            if self._cookie_collection != None:
                self.default_header['Cookie'] = self._cookie_collection
                
            if method == 'POST':
                if self._devel==True:
                    print 'Params:', set_params
            elif set_params != None:
                get_params = urllib.urlencode(set_params)
                print 'GET Params: ',get_params
                if '?' in self.url:
                    self.url+= '&' + get_params
                else:
                    self.url+= '?' + get_params
                    
            url_ = 'http://' + self.server + self.url
            if self._devel==True:
                print 'call URL: ',url_
            
            if method == 'POST':
                params = urllib.urlencode(set_params)
                req = Request(url_, data=params, headers=self.default_header)
            else:
                req = Request(url_, data=None, headers=self.default_header)
            
            fatal_error = None
            try:
                response = urlopen(req)
            except URLError, e:
                if hasattr(e, 'reason'):
                    fatal_error = 'We failed to reach a server.\n'
                    fatal_error+= 'Reason: ' + str(e.reason)
                    print 'e'*80
                    print fatal_error
                    
                elif hasattr(e, 'code'):
                    fatal_error = 'The server couldn\'t fulfill the request.\n'
                    fatal_error+= 'Error code: ' + str(e.code)
                    print 'e'*80
                    print fatal_error
            
            if fatal_error == None:
                the_page = response.read()
                
                #if self._cookie_collection == None:
                self._headers = response.headers.dict
                if self._headers.has_key('set-cookie'):
                    self.AddCookie(self._headers['set-cookie'])
                response.close()
                if self._devel==True:
                    print 'URL-LIB.'*10
                    print 'Response size:', len(the_page)
                        
                self.WriteTempFile(the_page)
                return the_page
            else:
                self.LogFatalError(fatal_error)
                return ''
        except Exception, msg:
            self.LogFatalError('Requester sie wywalil totalnie:' + str(msg))
            return ''
        