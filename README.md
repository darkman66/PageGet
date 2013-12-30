PageGet
=======

Python library which helps you downloading web pages (supports caching)

Example

    impor re

    P =.re.compile('href=[\'"](.*?)[\'"]', re.I)

    def getPage(self, url):
        r = Requester(False, True)
        r.SetFullUrl(url)
        data = r.GetRawPage(None, 'GET').replace('\t','').replace('\n','').replace('\r','')
        out = self.P.findall(data)
        print len(data), len(out)
        if len(out) > 0:
            print 'URL: %s' % out[0]
    
    getPage('http://www.google.com?q=python')
