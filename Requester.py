# -*- coding: utf-8 -*-

import gzip
import httplib
import urllib
import sys
import logging
import os
import gzip
import StringIO
from Log import Log
from pprint import pprint
from hashlib import md5
from datetime import datetime
from pprint import pformat
from RetryingCall import RetryingCall
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.web.client import getPage, downloadPage
from twisted.web.http_headers import Headers
from twisted.web import error, http
from pymongo.connection import Connection  
from gridfs import GridFS

class Requester:

    okErrs = (http.INTERNAL_SERVER_ERROR,
              http.BAD_GATEWAY,
              http.SERVICE_UNAVAILABLE)

    def __init__(self, _printing=False, _use_cache=False):
        self._cookie_collection = None
        self._headers = {}
        self.use_cache = _use_cache
        self.url = None
        self._devel =  _printing
        self.hash_name = None
        self.errCount = 1
        self.maxErrs = 10
        self.use_mongo = False
        
        self.default_header = {
                    'User-Agent' : 'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.8.1.3) Gecko/20061201 Firefox/2.0.0.3 (Ubuntu-feisty)',
                    #'Content-Type' : 'application/x-www-form-urlencoded',
                    'Connection' : 'keep-alive',
                    'Accept-Charset' : 'utf-8,ISO-8859-2,ISO-8859-1,q=0.7,*;q=0.7',
                    #'Accept-Encoding' : 'gzip,deflate',
                    #'Accept-Encoding' : 'gzip',
                    'Accept-Language' : 'pl,en-us;q=0.7,en;q=0.3',
                    'Accept' : 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5'
                }
        self.download_temp = 'download_temp/'

        # uchwyt dla loggera
        log = logging.getLogger()
        hldr = logging.StreamHandler(sys.stdout)
        hldr.setFormatter(logging.Formatter('%(asctime)s [%(name)-20s] %(levelname)-8s> %(message)s'))
        log.setLevel(logging.DEBUG if self._devel == True else logging.INFO)
        log.addHandler(hldr)

        self.log = Log('Requester', True)
        self.read_cache = True
        self.__initDownloadDir()


    def __initDownloadDir(self):
        if os.path.exists(self.download_temp) == False:
            self.log.info('creating download DIR: %s' % self.download_temp)
            os.mkdir(self.download_temp)


    def setDebug(self, status):
        self._devel = status

    def setTempDirName(self, dir_name):
        self.download_temp = dir_name.strip()
        self.__initDownloadDir()    

    def AddHeader(self, key, value):
        self.default_header[key] = value

    def SetUrl(self, server, url_to_call, port='80'):
        self.server    = server
        self.url       = url_to_call
        self.url_port  = port
        
    def getLastUrl2Call(self):
        return self.server + self.url
        
    def UseCache(self, status):
        self.use_cache = status
        if status == True and os.path.exists(self.download_temp) == False:
            os.mkdir(self.download_temp)


    def GetRawPage(self, set_params=None, method='POST'):
        params = ''
        if self._devel == True:
            print '> use_cache:' , self.use_cache
        temp_data = self.GetTempFile()
        if temp_data != None:
            return temp_data
        
        if self._devel:
            print '-'*80

        if self._cookie_collection != None:
            self.default_header['Cookie'] = self._cookie_collection
        
        extra_url_data = ''
        if method == 'POST':
            if self._devel:
                print 'Params:', params
        elif set_params != None:
            get_params = urllib.urlencode(set_params)
            print 'Params: ',get_params
            if '?' in self.url:
                self.url+= '&' + get_params
            else:
                self.url+= '?' + get_params
        
        
        self.log.info('URI call: %s -> %s' % (self.server, self.url))
        
        # jesli nie ma pliku trzeba go zrobic :)
        if set_params != None:
            params = urllib.urlencode(set_params)
        
        if self.url_port != '80':
            conn = httplib.HTTPConnection(self.server+':'+self.url_port)
        else:
            conn = httplib.HTTPConnection(self.server)
            
        if self._devel == True:
            conn.set_debuglevel(3)
        else:
            conn.set_debuglevel(0)
        
        
        conn.request(method, self.url, params, self.default_header)
        #for key in self.default_header:
        #    conn.putheader(key, self.default_header[key])
        self.log.debug('Zamkniecie polaczenia do stony...')
        response = conn.getresponse()
        
        
        self.log.debug('Status: %s, odpowiedz: %s' % (response.status, response.reason))
    
        data = response.read()
        gzip_val = self.GetRequestedKeyFromHeadersDict(response, 'content-encoding')
        if gzip_val == 'gzip':
            
            import StringIO
            compressedstream = StringIO.StringIO(data)
            zfile = gzip.GzipFile(fileobj=compressedstream)
            data = zfile.read()
            zfile.close()
        # sprawdz cookie!
        if self._devel:
            print 'cookie,'*10
            print self._cookie_collection
            print 'cookie,'*10
        
        if self._cookie_collection == None:
            self.AddCookie(self.GetRequestedKeyFromHeadersDict(response, 'set-cookie'))
        
        
        
        # zamkniecie polaczenia
        conn.close()
        # autoredirection
        if response.status in (302, 301):
            new_location = self.GetRequestedKeyFromHeadersDict(response, 'location')
            _use_cache = self.use_cache
            self.use_cache = False
            self.url = new_location
            
            if self._devel:
                print 'r'*80
                print 'REDIRECTION: ',self.url
                print 'r'*80
            # zamkniecie polaczenia
            data = self.GetRawPage(method='GET')
            self.use_cache = _use_cache
        
        # zapis do pliku co by nie walic tysiace zapytan
        self.WriteTempFile(data, self.hash_name)
        
        return data
    
    def GetRequestedKeyFromHeadersDict(self, resp, key):
        """
        Pobiera zadany klucz z headerow ostatniego zapytania
        """
        reps_headers = resp.getheaders()
        self._headers = reps_headers
        
        for item in reps_headers:
            if item[0].lower() == key.lower():
                return item[1]
        return None
            
    def GetLastHeaders(self):
        """
        Metoda zwraca headery z ostatniego zapytania
        """
        
        return self._headers
    
    
    def GetLastCookie(self):
        data = self.GetLastHeaders()
        for item in data:
            if item[0].upper() == 'SET-COOKIE':
                return item[1]
        return None
    
    def GetAllCookies(self):
        return self._cookie_collection

    def GetLastCallLink(self):
        return self.url
        

    def GetTempFile(self):
        """
        Metoda pobiera/zapisuje stronke pobierana do cache'u
        """
        data = None
        if self.use_cache == True:
            hash = md5(self.url)
            self.hash_name = hash.hexdigest()
            self.page_from_cache = False
            
#            f_name = self.download_temp + self.hash_name
            f_name = self.download_temp + self.hash_name + '.gz'
            if self._devel == True:
                print 'seek cache: ',f_name, '::', self.url
                
            # czy plik lokalny jest gz
            if os.path.exists(f_name.replace('.gz', '') ):
                data = open(f_name.replace('.gz', ''), 'rb').read()
                f = gzip.open(f_name, 'wb')
                f.write(data)
                f.close()
                os.unlink( f_name.replace('.gz', '') )
                return data
            
            # teraz odczyt pliku gzip
            if self.read_cache == True:
                if self.use_mongo == True:
                    try:
                        connection = Connection("localhost", 27017)
                        db = connection['parser']

                        fs = GridFS(db)
                        fp = fs.open(self.hash_name , 'r', self.download_temp.replace('/', '') )
                        f = gzip.GzipFile(fileobj=fp, mode='rb')
                        data = f.read()
                        f.close()
                        fp.close()
                        del(f)
                        connection.disconnect()

                    except Exception, e:
                        print 'read cahce error: ', e
                        self.page_from_cache = False
                        return None

                elif os.path.exists(f_name):
                        f = gzip.open(f_name, 'rb')
                        data = f.read()
                        f.close()
            else:
                data = ''
                    
            if self._devel == True:
                print '# Found cache: ', self.hash_name
            self.page_from_cache = True
        
        return data
    
    def IsPageFromCache(self):
        """
        Metoda zwraca True gdy ostatnio wolana strona pochodzila z cache podrecznego!
        """
        return self.page_from_cache
    
    def ReturnLastHashName(self):
        return self.hash_name
    
    def GetHashFileContent(self, h_name):
        data = None
        if self.use_cache == True:
            f = open (self.download_temp+h_name, 'r')
            data = f.read()
            f.close()
        
        return data

    def WriteTempFile(self, data, hash_name=None):
        
        if self.use_cache == True:
            if hash_name is None:
                hash = md5(self.url )
                hash_name = hash.hexdigest()
                self.last_hash_name = hash_name
                
            self.log.debug('write file to cache: ', hash_name)
            self.log.debug('use mongo: %s' % self.use_mongo)
#            open(self.download_temp+hash_name, 'wb').write(data)
            if self.use_mongo == False: 
                f_name = self.download_temp + hash_name + '.gz'
                f = gzip.open(f_name, 'wb')
                f.write(data)
                f.close()

            if self.use_mongo == True:
                connection = Connection("localhost", 27017)
                db = connection['parser']

                s = StringIO.StringIO()
                f = gzip.GzipFile(fileobj=s, mode='wb')
                f.write(data)
                f.close()
                val = s.getvalue()
                s.close()
                del (s)
                del (f)

                fs = GridFS(db)
                fp = fs.open(hash_name , 'w', self.download_temp.replace('/', '') )
                fp.write(val)
                fp.close()
                connection.disconnect()



    def DownloadFile(self, url_addr):
        try:
            print 'Try to download file:',url_addr
            
            f = urllib.urlopen(url_addr)
            data = f.read()
            f.close()
        
            return data
        except Exception, e:
            print 'FAIL!', str(e)
            return None
        
        
        
    def UrlStringTODict(self, str_dict):
        """
        Metoda zamieniazakodowany slownik w postaci URI
        na postac slowniak pythona
        """
        raw_data = urllib.unquote(str_dict.strip())
        a = raw_data.split('&')
        r_dict = {}
        for item in a:
            b = a.split('=')
            r_dict[b[0]] = b[1]
            
        return r_dict
    
    def AddCookie(self, value):
        if value != None:
            if self._cookie_collection == None:
                self.log.debug('!!!! New cookie value:', value)
                if '%' in value:
                    self._cookie_collection = urllib.unquote(urllib.unquote(urllib.unquote(value)))
                else:
                    self._cookie_collection = str(value)
            else:
                self.log.debug('[COOKIE ADD] Adding cookie', value)
                if '%' in value:
                    self._cookie_collection+= ' '+urllib.unquote(urllib.unquote(urllib.unquote(value)))
                else:
                    self._cookie_collection+= ' '+value
                    
                    
    def SetFullUrl(self, full_url):
        """
        Metoda otrzymuje url jak http://www.wp.pl/costam/metoda/costam.html
        i wydlubuje server oraz posostaly shit
        """
        self.full_url = full_url
        url = full_url.replace('http://','').replace('www.','')
        d = url.split('/')
        server = d[0]
        path = ''
        for item in d[1:]:
            path+= '/' + item
        
        self.SetUrl(server, path)
        
    def SetFullUrl2(self, full_url):
        """
        Metoda otrzymuje url jak http://www.wp.pl/costam/metoda/costam.html
        i wydlubuje server oraz posostaly shit
        """
        url = full_url.replace('http://','')
        d = url.split('/')
        server = d[0]
        path = ''
        for item in d[1:]:
            path+= '/' + item
        
        self.SetUrl(server, path)  
        
    def LogFatalError(self, message):
        try:
            now = str(datetime.now())[:-7]
            f = open('logs/requester', 'a')
            f.write(now + ' FATAL ERROR: '+message + '\n\n')
            f.close()
        except Exception, msg:
            self.log.error('!!!! FATAL ERROR:', msg)
           

    def wget(self, url):
        self.use_cache = True
        self.SetFullUrl(url)
        temp_data = self.GetTempFile()
        if temp_data is not None:
            return temp_data

        hash = md5(self.url)
        hash_name = hash.hexdigest()
        self.last_hash_name = hash_name
        cmd = 'wget --output-document=%s%s %s' % (self.download_temp, hash_name, url)
        self.log.debug(os.popen(cmd))
        data = self.GetTempFile()
        self.log.debug(len(data))

        return data



    def getPageTwisted(self, url, set_params=None, method='GET'):
        self.SetFullUrl(url)
        # strona z cache'u
        self.log.debug('> use_cache:' , self.use_cache)

        temp_data = self.GetTempFile()
        if temp_data != None:
            d = Deferred()
            d.addCallback(self.__cb_tempData, temp_data)
            d.addErrback(self.__eb_tempData)
            
            return d
        f_name = self.download_temp + self.hash_name
        d = downloadPage(url, f_name)
        d.addCallback(self.__cb_Response)
        d.addErrback(self.__eb_ErrResponse)
        
#        r = RetryingCall(getPage, url)
#        d = r.start(failureTester=TwitterFailureTester())
#        d.addCallback(self.__cb_Response)
#        d.addErrback(self.__eb_ErrResponse)
        return d
    
    
    def __eb_tempData(self, e):
        e.trap(error.Error)
        self.errCount += 1
        if (self.errCount < self.maxErrs and int(e.value.status) in self.okErrs):
            self.log.info('retry to get page %s/%s' % (self.errCount,self.maxErrs))
            self.getPageTwisted(self.full_url)
        else:
            self.log.error('__eb_tempData: %s' % e)
            return e
    
    
    def __cb_tempData(self, data):
        self.log.info('__cb_tempData: page data from cache: %s' % len(data))
        return data
    
    def __cb_Response(self, results):
        self.log.debug('%s' % results)
        try:
            for isSuccess, content in results:
                self.log.debug("Successful? %s" % isSuccess)
                self.log.debug("Content Length: %s" % len(content))
                # zapis do pliku co by nie walic tysiace zapytan
#                print 'DATA: ', type(results)
#                self.WriteTempFile(results, self.hash_name)
                
                r = results.replace('\r','').replace('\t', '')
                
                return r
            
        except Exception, e:
            print 'walnieta obrioka odpowiedzi: ' % e
            
        return ''
    
    def __eb_ErrResponse(self, e):
        self.log.error('!!! nieudana proba pobrania strony!!! %s' % e)
        self.log.error(e.trap(error.Error))
        self.errCount += 1
        self.log.info('[ retry to get page %s/%s]' % (self.errCount,self.maxErrs))

        if (self.errCount < self.maxErrs and int(e.value.status) in self.okErrs):
            self.getPageTwisted(self.full_url)
        else:
            self.log.error('eeee: %s' % e)
            return e



class TwitterFailureTester(object):
    okErrs = (http.INTERNAL_SERVER_ERROR,
              http.BAD_GATEWAY,
              http.SERVICE_UNAVAILABLE)
    def __init__(self):
        self.seen404 = False

    def __call__(self, failure):
        failure.trap(error.Error)
        status = int(failure.value.status)
        self.log.debug('status: %s' % status)

        if status == http.NOT_FOUND:
            if self.seen404:
                return failure
            else:
                self.seen404 = True
        elif status not in self.okErrs:
            return failure
        
        
