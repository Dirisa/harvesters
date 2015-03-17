import logging
import time
import traceback
from StringIO import StringIO
import sys
import urllib
import urllib2
import base64
from urlparse import urlparse
from ZipUtil import ZipUtil


class HTTPTransport:
    def __init__(self, url,username="", password=""):
        """
        """
        if url.lower().startswith('http://'):
            parts = url[7:].split('/')
            # TODO: Temporary fix.
            if False:#not parts[1].startswith(':'):
                url = 'http://%s:80/%s' %(parts[0], '/'.join(parts[1:]))
            else:
                url = 'http://%s/%s' %(parts[0], '/'.join(parts[1:]))
        self.url = url
        self.username = username
        self.password = password   
        self.contentType = "" 
        self.message = ""
        
        self.files = self.getFile()
    
    def getFile(self):
        """
        @summary: a dict with file data as key and file path as value
        """  
        try: 
            if (self.username == "") and (self.password == ""):
                res = urllib.urlopen(self.url)
                self.contentType = res.info()["Content-Type"]
                #print 'Get HTTP File anonymously. headers = ' + str(res.info())
            else:
                req = urllib2.Request(self.url)
                base64string = base64.encodestring('%s:%s' % (self.username, self.password))[:-1]
                authheader =  "Basic %s" % base64string
                req.add_header("Authorization", authheader)
                res = urllib2.urlopen(req)
                self.contentType = res.info()["Content-Type"]
                print 'Get Authenticated File.'
            print 'self.contentType: ', self.contentType

            if self.contentType.lower().find("zip") != -1:
                tDict = {}
                f = StringIO()
                f.write(res.read())
                util = ZipUtil(f)
                theFiles = util.getFileContentWithExtension("xml")
                for f in theFiles:
                    tDict[f[1]] = self.url +"/"+ f[0]
                return tDict
            elif self.contentType.lower().find("xml") != -1:
                return {res.read():self.url}
            elif self.contentType.lower().startswith("text/html") != -1:
                html = res.read()
                #print 'HTML = %s' % html
                if html.find('Please log in') != -1:
                    self.message = "It looks like the resource requires a password - ensure public access to the resource or correct credentials in the harvester"
                else:
                    self.message = "Invalid Doc Type - check that the resource exists and it is the correct type"
                #print 'MSG = %s' % self.message
                return {}
            else:
                print 'Return unknown.'
                result = res.read()
                return {result:self.url}
            self.message = "Invalid URL or Wrong Content Type"
            return {}
        except urllib2.URLError, e:
            logging.error('HTTPTransport getFile (%s): %s' % \
                (str(self.url), str(e)))
            self.message = "Cannot access file - HTTP"
            return {}
        except IOError, e:
            logging.error('IOERROR getFile: %s' % str(e))
            self.message = "Cannot acces file - IO"
            return {}
        except:
            traceback.print_exc(file=sys.stdout)
            io = StringIO()
            traceback.print_exc(file=io)
            io.seek(0)            
            trace = io.read()
            self.message = trace            
            return {}
    
if __name__ == "__main__":    
    #trans = HTTPTransport("http://127.0.0.1/test/Provinces.xml","","")
    trans = HTTPTransport("http://data.saeon.ac.za/uploads/ACEP_SANS.zip/at_download/file", "", "")
    print 'Message: %s' % trans.message
    print 'File: %s' % trans.getFile()
    print 'contentType: %s' % trans.contentType
    
    
    

