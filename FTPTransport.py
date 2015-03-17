import ftplib
from ftplib import FTP
import time
import traceback
from StringIO import StringIO
import sys
from ZipUtil import ZipUtil

class FTPTransport:
    def __init__(self, url, username="", password="", port=21):
        """
        """
        self.username = username
        self.password = password
        self.url = url
        self.port = port
        self.con = None 
        self.host = None
        self.path = "" # the path tp download or harvert
        self.isFile = True;
        self.switchedMode = False;
        
        self._parseURL(self.url)   
        self.message = ""
        self.connect()   
    
    def _parseURL(self,url):
        """
        """
        tmp = url.replace("ftp://","")
        parts = tmp.split("/")
        self.host = parts[0]
        self.path = "/".join(parts[1:])
        
        if (url[-4:] == ".xml") or (url[-4:] == ".zip"):
            self.isFile = True
        else:
            self.isFile = False                    
    
    def getFiles(self):
        """
        @return: a dict with file data as key and extension as value
        """
        try:
            if self.con == None:
                return 
            
            files = {}
            if self.isFile:
                files = self.downloadFile(self.path)            
            else:
                self.con.cwd(self.path)
                files = self.downloadFiles()            
            return files
        except Exception, e:
            print e
            self.message('FTP getFiles failed (switched=%s): Exception\n%s' % (
                self.switchedMode, e))
            # We try once to switch the mode to active
            if not self.switchedMode:
                self.con.set_pasv(False)
                self.switchedMode = True
                return self.getFiles()

    
    def downloadFiles(self, filters=[".xml",".zip"]):
        """
        @summary: where the url points to a folder to be harvested
        @return: a dict with file data as key and file path as value
        @param filters: is a list of file extensions to filter downloads on
        """                 
        retDict = {}
        fileList = self.getFileList()
        fileList = [x for x in fileList if x[-4:] in filters]
        for file in fileList:            
            fDict = self.downloadFile(file)
            for k in fDict.keys():
                retDict[k] = fDict[k]
        return retDict
            
    def downloadFile(self, filename):
        """
        @summary: where the url given is a single file to download
        @return: a dict with file data as key and file path as value
        @param filename: the fileName to download
        """        
        f2 = StringIO()
        self.con.retrbinary("RETR " + filename,f2.write)
        f2.seek(0)
        
        if filename.lower()[-4:] == ".zip":
            util = ZipUtil(f2)
            theFiles = util.getFileContentWithExtension("xml")        
            tDict = {}
            for f in theFiles:
                tDict[f[1]] = self.url +"/"+ f[0]                
            return tDict
        else:            
            return {f2.read():  self.url +"/"+ filename}        
            
    def connect(self):
        """
        """              
        if self.con == None:
            self.con = FTP()
            try: 
                self.con.connect(self.host, self.port)
            except:
                self.con = None                
                self.message = "Invalid FTP URL or Port"
            try:
                self.con.login(self.username, self.password)                
            except:
                self.con = None                
                self.message += " - Invalid login details"
        return self.con           
    
    def getFileListForPath(self, path):
        """
        @summary: returns a list of files for the given path on the ftp server
        """
        if self.con != None:
            self.con.cwd(path)
            return self.getFileList()        
    
    def getFileList(self):
        """
        @return: a list of file names for the current directory
        """
        if self.con != None:
            fList = self.con.nlst()        
            return fList    
    
if __name__ == "__main__":
    #trans = FTPTransport("ftp://127.0.0.1/xml","test","test")    
    trans = FTPTransport("ftp://127.0.0.1/xml/","test","test")
    #trans = FTPTransport("ftp://10.50.130.20/","csir","c51r2048")
    #trans = FTPTransport("ftp://10.50.130.116/","test","test") 
    #l = trans.getFiles()
    #print l
    
    
        
