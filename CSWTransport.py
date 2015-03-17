import logging
import string
import urllib2
import urllib
import cookielib
import cgi
import httplib
import datetime
import time
from xml.dom import minidom
import random
from StringIO import StringIO
import sys
import traceback

recordCountTemplate = """<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
    service="CSW" 
    version="2.0.2">
    <csw:Query typeNames="csw:Record">
        <csw:Constraint version="1.1.0">
            <Filter xmlns="http://www.opengis.net/ogc" 
                    xmlns:gml="http://www.opengis.net/gml"/>
        </csw:Constraint>
    </csw:Query>
</csw:GetRecords>"""

recordCountNewTemplate = """<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords 
    xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
    xmlns:gmd="http://www.isotc211.org/2005/gmd"
    xmlns:ogc="http://www.opengis.net/ogc"
    service="CSW"
    version="2.0.2"
    resultType="hits"
    outputSchema="http://www.isotc211.org/2005/gmd">
    <csw:Query typeNames="gmd:MD_Metadata">
       <csw:ElementSetName typeNames="gmd:MD_Metadata">brief</csw:ElementSetName>
       <csw:Constraint version="1.1.0">
            <ogc:Filter> 
                <ogc:PropertyIsLike wildCard="*" singleChar="?" escapeChar="">
                  <ogc:PropertyName>csw:AnyText</ogc:PropertyName>
                  <ogc:Literal>*</ogc:Literal>
                </ogc:PropertyIsLike>
            </ogc:Filter> 
       </csw:Constraint>
    </csw:Query>
</csw:GetRecords>"""

recordByIdTemplate = """<?xml version="1.0"?>
<csw:GetRecordById xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
  service="CSW" outputSchema="csw:IsoRecord" version="2.0.2">
    <csw:Id>%s</csw:Id>
  <csw:ElementSetName>full</csw:ElementSetName>
</csw:GetRecordById> """

recordByIdNewTemplate = """<?xml version="1.0"?>
<csw:GetRecordById xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
  service="CSW" outputSchema="http://www.isotc211.org/2005/gmd"
  version="2.0.2">
    <csw:Id>%s</csw:Id>
    <csw:ElementSetName>full</csw:ElementSetName>
</csw:GetRecordById> """

recordAllTemplate = """<?xml version="1.0"?>
<csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2"
    xmlns:gmd="http://www.isotc211.org/2005/gmd" 
    service="CSW" version="2.0.2" 
    resultType="results"
    maxRecords="%s">
    <csw:Query typeNames="gmd:MD_Metadata">
        <csw:Constraint version="1.1.0">
            <Filter xmlns="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml"/>
        </csw:Constraint>
        <csw:ElementSetName>brief</csw:ElementSetName>
    </csw:Query>
</csw:GetRecords> """

recordAllNewTemplate = """<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords 
    xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
    xmlns:gmd="http://www.isotc211.org/2005/gmd"
    xmlns:ogc="http://www.opengis.net/ogc" 
    service="CSW"
    version="2.0.2"
    resultType="results"
    startPosition="1"
    maxRecords="%(matched)s"
    outputSchema="http://www.isotc211.org/2005/gmd">
    <csw:Query typeNames="gmd:MD_Metadata">
       <csw:ElementSetName typeNames="gmd:MD_Metadata">full</csw:ElementSetName>
       <csw:Constraint version="1.1.0">
          <ogc:Filter>
            <ogc:PropertyIsLike wildCard="*" singleChar="?" escapeChar="">
              <ogc:PropertyName>csw:AnyText</ogc:PropertyName>
              <ogc:Literal>*</ogc:Literal>
            </ogc:PropertyIsLike>
          </ogc:Filter>
       </csw:Constraint>
    </csw:Query>
</csw:GetRecords>"""

recordCountTemplateByType = """<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
    service="CSW" version="2.0.2">
    <csw:Query typeNames="csw:Record">
        <csw:Constraint version="1.1.0">
          <Filter xmlns="http://www.opengis.net/ogc" 
                  xmlns:gml="http://www.opengis.net/gml">
            <PropertyIsLike wildCard="%%" singleChar="_" escapeChar="">
              <PropertyName>type</PropertyName>
              <Literal>%%%s%%</Literal>
            </PropertyIsLike>
          </Filter>
        </csw:Constraint>
    </csw:Query>
</csw:GetRecords>"""

recordAllTemplateByType = """<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
    service="CSW" version="2.0.2"
    resultType="results"
    startPosition="1" 
    maxRecords="%s">
    <csw:Query typeNames="csw:Record">
        <csw:ElementSetName>brief</csw:ElementSetName>
        <csw:Constraint version="1.1.0">
          <Filter xmlns="http://www.opengis.net/ogc" 
                  xmlns:gml="http://www.opengis.net/gml">
            <PropertyIsLike wildCard="%%" singleChar="_" escapeChar="">
              <PropertyName>type</PropertyName>
              <Literal>%%%s%%</Literal>
            </PropertyIsLike>
          </Filter>
        </csw:Constraint>
    </csw:Query>
</csw:GetRecords>"""

xmlHeader = """<?xml version="1.0" encoding="UTF-8"?>"""
def insertAttribute(orig, after, new):
    idx = orig.find(after)
    if idx < 0:
        return orig

    idx = idx + len(after)
    return "%s %s %s" % (orig[:idx], new, orig[idx:])

class CSWTransport:
    """
    """    
    
    def __init__(self, url, xmlDumpPath, username="", password="", 
                  standard="", transport=""):
        self.url = url;
        self.path = xmlDumpPath;
        self.message = None
        self.username = username
        self.password = password
        self.standard = standard
        self.transport = transport
            
    def getRecords(self):
        """
        @summary: requests all the xml metadata from the given CSW site url and returns the records
        @return: a dict with file data as key and record id as value
        """  
        #self.cswLogout()
        opener = None
        #opener = self.cswLogin()

        ids = self._getAllRecordIds(opener)
        retDict = {}
        for id in ids:
            if self.transport == 'CSW-SANSA':
                query = recordByIdNewTemplate % id
            else:
                query = recordByIdTemplate % id

            xmlRes = self.getPostContent(xml_request=query, opener=opener)            
            if not xmlRes:
                return
            tDom = minidom.parseString(xmlRes)
            elms = tDom.getElementsByTagName("csw:GetRecordByIdResponse")           
            if elms: 
              tRes = None
              for node in elms[0].childNodes:                
                  if node.nodeType != node.TEXT_NODE:
                      tRes = node.toxml().encode('ascii', 'ignore')
                      tRes = \
                        """<?xml version="1.0" encoding="UTF-8"?>\n""" + tRes
                      if self.transport == 'CSW-SANSA':
                          xmlnss = [
                            'xmlns:gco="http://www.isotc211.org/2005/gco"',
                            'xmlns:gmd="http://www.isotc211.org/2005/gmd"',
                            'xmlns:gml="http://www.opengis.net/gml"',
                            'xmlns:gts="http://www.isotc211.org/2005/gts"',
                            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
                          ]
                          after = "gmd:MD_Metadata"
                          for xmlns in xmlnss:
                              idx = tRes.find(xmlns)
                              if idx < 0:
                                  tRes = insertAttribute(tRes, after, xmlns)

                      break
              if tRes:
                  retDict[tRes] = id           
        #self.cswLogout(opener)
        return retDict

    def getRecordForId(self, recordId, opener):
        """
        @summary: get the metadata for the given record id
        @param recordId: record id to get metadata for
        """
        return self.getPostContent(recordByIdTemplate % id, opener)
                    
    def _getAllRecordIds(self, opener):
        """
        @return: returns a list of metadata record ids from the csw service
        """
        num = self.getRecordCountForServer(opener)

        retList = []
        if num == 0:
            if not self.message:
                self.message = "CSW request returned zero records"
        else:
            if self.transport == 'CSW-NEW':
                resXml = self.getPostContent(
                          recordAllTemplateByType % (num, ''), #TODO self.standard),
                          opener) 
                d = minidom.parseString(resXml)
                elms = d.getElementsByTagName("dc:identifier")        
                for elm in elms:
                    retList.append(elm.firstChild.nodeValue)
            elif self.transport == 'CSW-SANSA':
                resXml = self.getPostContent(
                            recordAllNewTemplate % {'matched': num }, opener) 
                d = minidom.parseString(resXml)
                elms = d.getElementsByTagName("gmd:fileIdentifier")        
                for elm in elms:
                    ids = elm.getElementsByTagName("gco:CharacterString")
                    retList.append(ids[0].firstChild.nodeValue)
                logging.debug('!!!!!! Returned %s Ids:%s' % (
                    len(retList), str(retList)))
            else:
                resXml = self.getPostContent(
                            recordAllTemplate % num, opener) 
                d = minidom.parseString(resXml)
                elms = d.getElementsByTagName("dc:identifier")        
                for elm in elms:
                    retList.append(elm.firstChild.nodeValue)
                
        return retList   


    def getRecordCountForServer(self, opener):
        """
        @summary: queries the CSW service for a count of records on the server exposed to the csw service
        @return: the count of records returned from the query
        """
        if self.transport == 'CSW-NEW':
            query = recordCountTemplateByType % ''
        elif self.transport == 'CSW-SANSA':
            query = recordCountNewTemplate
        else:
            query = recordCountTemplate

        resXml = self.getPostContent(query, opener)
        matched = 0
        if self.message:
            self.message = '%s\n\nQuery used: %s\n' % (self.message, query)
            logging.info('getRecordCountForServer error: %s' % self.message)
            return matched
        if resXml:
            d = minidom.parseString(resXml)
            elms = d.getElementsByTagName("csw:SearchResults")
            if len(elms) > 0:
                matched = elms[0].getAttribute("numberOfRecordsMatched")
                if type(matched) != int:
                    matched = int(matched)
        logging.debug('getRecordCountForServer: %s matched' % matched)
        return matched    

    def cswLogin(self):    
        """
        @summary: does a post to the url to login
        @param url: the url to post to
        """
        opener = None

        try:
          # GeoNetwork constants
          # pull from url
          #http://www.saeonocean.co.za/geonetwork/srv/en/test.csw
          url_in = self.url
          if self.transport == 'CSW':
              gn_loginURI = "/geonetwork/srv/en/xml.user.login"
              url_in = url_in + gn_loginURI
          # authentication Post parameters
          creds = {}
          if self.username:
              creds['username'] = self.username
          if self.password:
              creds['password'] = self.password
          post_parameters = urllib.urlencode(creds)

          # send authentication request
          request = urllib2.Request(url_in, post_parameters)
          response = urllib2.urlopen(request)
          logging.debug("login response: %s" % response.read())

          # a basic memory-only cookie jar instance
          cookies = cookielib.CookieJar()
          cookies.extract_cookies(response,request)
          cookie_handler= urllib2.HTTPCookieProcessor( cookies )
          # a redirect handler
          redirect_handler= urllib2.HTTPRedirectHandler()
          # save cookie and redirect handler for future HTTP Posts
          if self.transport == 'CSW':
              opener = urllib2.build_opener(redirect_handler,cookie_handler)
        except Exception, e:
            logging.debug("CSW Login error: %s" % str(e))
            self.message = "CSW Login error: %s" % str(e)
            return {}

        return opener


    def cswLogout(self, opener=None):    
        """
        @summary: does a post to the url to logout
        @param url: the url to post to
        """
        if self.transport != 'CSW':
            return
        try:
          # GeoNetwork constants
          # pull from url
          #http://www.saeonocean.co.za/geonetwork/srv/en/test.csw
          url_out = self.url
          if self.transport == 'CSW':
              gn_logoutURI = "/geonetwork/srv/en/xml.user.logout"
              url_out = url_out + gn_logoutURI

          # authentication Post parameters
          creds = {}
          if self.username:
              creds['username'] = self.username
          if self.password:
              creds['password'] = self.password
          post_parameters = urllib.urlencode(creds)

          request = urllib2.Request(url_out)
          if opener:
            response = opener.open(request)
          else:
            response = urllib2.urlopen(request)
          logging.debug("Logout response: %s" % response.read())
        except Exception, e:
            logging.info(
                "CSW Logout error: Url=%s Error=%s" % (url_out, str(e)))
            self.message = \
                "CSW Logout error: Url=%s Error=%s" % (url_out, str(e))
            return {}


    def _getExceptionResponse(self, xml_response):
        tDom = minidom.parseString(xml_response)
        elms = tDom.getElementsByTagName("ows:ExceptionText")           
        if not elms:
            #No exceptios
            return None
        return elms[0].firstChild.nodeValue             


    def getPostContent(self, xml_request='', opener=None):             
        """
        @summary: does a post to the url with the given data and returns the result
        @param url: the url to post to
        @param data: the data to post to the url
        """
        try:
          # GeoNetwork constants
          # pull from url
          #http://www.saeonocean.co.za/geonetwork/srv/en/test.csw
          url_csw = self.url
          if self.transport == 'CSW':
              gn_cswURI = "/geonetwork/srv/en/csw"
              url_csw = url_csw + gn_cswURI

          # HTTP header for authentication
          header_urlencode = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
          # HTTP header for CSW request
          header_xml = {"Content-type": "text/xml", "Accept": "text/xml"}
          # authentication Post parameters
          creds = {}
          if self.username:
              creds['username'] = self.username
          if self.password:
              creds['password'] = self.password
          post_parameters = urllib.urlencode(creds)

          # CSW request
          logging.debug("\nRequest\nUrl:  %s\n'%s'\n" % (url_csw, str(xml_request)))
          request = urllib2.Request(url_csw, xml_request, header_xml)
          if opener:
              response = opener.open(request)
          else:
              response = urllib2.urlopen(request)
          # CSW respons
          xml_response = response.read()
          logging.debug("\nSearch response:\n '%s'\n" % str(xml_response))

          exception = self._getExceptionResponse(xml_response)
          if exception:
              self.message = exception
              return

          #TODO HACK #return xml_response[xml_response.index('<'):]
          return xml_response

        except urllib2.URLError, e:
            #logging.debug("Error with CSW request: %s" % str(e.reason[1]))
            #self.message = "Error with CSW request: %s" % str(e.reason[1])
            logging.info("Error with CSW request: %s" % str(e))
            self.message = "Error with CSW request: %s" % str(e)
            return
        except Exception, e:
            logging.info("Error with CSW request: %s" % str(e))
            self.message = "Error with CSW request: %s" % str(e)
            return
           
    def _getTimestampName(self):
        """
        @summary: generates a random/unique name and returns the name
        @return: a name for an xml file
        """
        tNum = random.randint(1, 99999)
        return str(time.time()).replace(".","") + str(tNum) + ".xml"    
    
    def getAllRecordAndWriteToFile(self):
        """
        @summary: does a query to the CSW service for all records and writes the results to file
        """
        recs = self.getRecords()
        print 'getAllRecordAndWriteToFile', len(recs)
        #for rec in recs.keys():            
        #    f = file(self.path + "/" + self._getTimestampName(),"w")
        #    f.write(rec)
        #    f.close()
        

if __name__ == "__main__":    
    #postUrl = "http://www.saeonocean.co.za/geonetwork/srv/en/csw?"
    postUrl = "http://41.74.158.4/csw"
    #postUrl = "http://196.213.187.51/PLATFORM_2/MAP/csw.asp"
    #postUrl = "http://delta.icc.es/indicio/csw?"
    #proxies = {'http': 'http://10.50.130.26:8080'}

    h = CSWTransport(url=postUrl, xmlDumpPath="c:/tmp", transport="CSW-NEW")
    #h.getAllRecordAndWriteToFile()    
    print "========================="
    print h.getAllRecordAndWriteToFile()
    #count = h.getRecordCountForServer(postUrl);
    #h.harvest()  
    



