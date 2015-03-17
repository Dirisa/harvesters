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


class OAITransport:
    """
    """    
    
    def __init__(self, url, xmlDumpPath="", username="", password="", 
                  standard="", transport="", substitutionUrl=None):
        self.url = url;
        self.path = xmlDumpPath;
        self.message = ""
        self.username = username
        self.password = password
        self.standard = standard
        self.transport = transport
        self.substitutionUrl = substitutionUrl
            
    def getRecords(self):
        """
        @summary: requests all the xml metadata from the given OAI site url and returns the records
        @return: a dict with file data as key and record id as value
        """  
        opener = None
        #self.oaiLogout()
        #opener = self.oaiLogin()
        ids = self._getAllRecordIds(opener)
        retDict = {}
        for id in ids:
            params = {'verb':'GetRecord',
                      'identifier': id }
            if self.standard == 'DublinCore' or self.transport == 'OAI-Metacat':
                params['metadataPrefix'] = 'oai_dc'

            xmlRes = self.getPostContent(param_dict=params, opener=opener)            
            tDom = minidom.parseString(xmlRes)
            elms = tDom.getElementsByTagName("metadata")           
            if elms: 
                tRes = None
                for node in elms[0].childNodes:                
                    if node.nodeType != node.TEXT_NODE:
                        tRes = node.toxml().encode('ascii', 'ignore')
                        tRes = \
                            """<?xml version="1.0" encoding="UTF-8"?>""" + tRes
                        if self.transport == 'OAI-Metacat':
                            idelms = tDom.getElementsByTagName("dc:identifier")
                            docid = None
                            for id_elm in idelms:
                                if id_elm.childNodes:
                                    val = id_elm.childNodes[0].nodeValue
                                    if not val.startswith('http'):
                                        docid = val
                            if docid:
                                tRes = self.getPostContent(
                                    url='http://saeonmetacat.co.za/knb/metacat',
                                    param_dict={'action':'read', 
                                                'qformat': 'xml',
                                                'sessionid':0,
                                                'docid': docid},
                                    opener=opener)            
                        break
                if tRes:
                    retDict[tRes] = id           

        #self.oaiLogout(opener)
        print 'getRecords return %s' %  len(retDict.keys())
        return retDict

    def _getAllRecordIds(self, opener):
        """
        @return: returns a list of metadata record ids from the csw service
        """
        retList = []
        params = {'verb':'ListIdentifiers'}
        if self.standard == 'DublinCore' or self.transport == 'OAI-Metacat':
            params['metadataPrefix'] = 'oai_dc'
        resumptionToken = None
        counter = 0
        while True:
            counter += 1
            #TODO: HACK to limit records to about 10
            #if counter > 2:
            #    break
            if resumptionToken:
                params['resumptionToken'] = resumptionToken
                if 'metadataPrefix' in params:
                    del params['metadataPrefix']
            else:
                print 'check if param exists' #TODO    

            resXml = self.getPostContent(param_dict=params, opener=opener) 

            d = minidom.parseString(resXml)
            elms = d.getElementsByTagName("identifier")        
            for elm in elms:
                retList.append(elm.firstChild.nodeValue)
            tokens = d.getElementsByTagName("resumptionToken")        
            if len(tokens) == 0:
                break

            token = tokens[0]
            try:
                resumptionToken = token.firstChild.nodeValue
                logging.info("Total Records = %s" % \
                    token.getAttribute('completeListSize'))
                if not resumptionToken:
                    break
            except:
                logging.info('Failed getting resumptionToken')
                break
            
        print "_getAllRecordIds return %s Ids" % len(retList)
        return retList   


    def getPostContent(self, url=None, param_dict='', opener=None):             
        """
        @summary: does a post to the url with the given data and 
                  returns the result
        @param url: the url to post to
        @param data: the data to post to the url
        """
        try:
          # OAI request
          if url is None:
              url = self.url
          logging.info("\nRequest: %s\n'%s'\n" % (url, str(param_dict)))
          params = urllib.urlencode(param_dict)
          request = urllib2.Request(url, params)
          if opener:
            response = opener.open(request)
          else:
            response = urllib2.urlopen(request)
          # OAI respons
          xml_response = response.read()
          logging.info("\nSearch response:\n '%s'\n" % str(xml_response))

          return xml_response

        except Exception, e:
          logging.info("Error with OAI request: %s" % str(e))
          raise e
           
    def _getTimestampName(self):
        """
        @summary: generates a random/unique name and returns the name
        @return: a name for an xml file
        """
        tNum = random.randint(1, 99999)
        return str(time.time()).replace(".","") + str(tNum) + ".xml"    
    
    def getAllRecordAndWriteToFile(self):
        """
        @summary: does a query to the OAI service for all records 
                  and writes the results to file
        """
        recs = self.getRecords()
        print 'getAllRecordAndWriteToFile', len(recs)
        #for rec in recs.keys():            
        #    f = file(self.path + "/" + self._getTimestampName(),"w")
        #    f.write(rec)
        #    f.close()
        

if __name__ == "__main__":    
    #postUrl = "http://metacat.lternet.edu/knb/dataProvider?verb=GetRecord&metadataPrefix=oai_dc&identifier=urn:lsid:knb.ecoinformatics.org:knb-lter-gce:26"
    postUrl = "http://metacat.lternet.edu/knb/dataProvider"

    print "========================="
    h = OAITransport(postUrl, "c:/tmp", standard="DublinCore", transport="OAI")
    #print h.getAllRecordAndWriteToFile()
    print "========================="
    



