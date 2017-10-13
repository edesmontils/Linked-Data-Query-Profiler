#!/usr/bin/env python3.6
# coding: utf8
"""
Basic endpoint wrappers for SPARQL et TPF
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

from pprint import pprint

import re
import json
import logging
import hashlib

import csv
import subprocess
import os.path
import socket
import sys
import multiprocessing as mp

from SPARQLWrapper import SPARQLWrapper, JSON #, SPARQLWrapperException
from SPARQLWrapper.Wrapper import QueryResult, QueryBadFormed, EndPointNotFound, EndPointInternalError

#==================================================
#==================================================

class EndpointException(Exception):
	'''raise when the endpoint can't answer normally to the query (timeout...). Syntax errors are right answers'''

class QueryBadFormed(EndpointException):
    """docstring for QueryBadFormed"""
    def __init__(self, arg):
        super(QueryBadFormed, self).__init__()
        self.arg = arg

class TPFClientError(EndpointException):
    """docstring for TPFClientError"""
    def __init__(self, arg):
        super(TPFClientError, self).__init__(arg)

class TimeOut(EndpointException):
    """docstring for TimeOutException"""
    def __init__(self, arg):
        super(TimeOutException, self).__init__(arg)
        
#==================================================
#==================================================

EP_QueryBadFormed = False
EP_QueryWellFormed = True

DEFAULT_TPF_EP = 'http://localhost:5001' # http://172.16.9.3:5001/dbpedia_3_9   http://localhost:5001/dbpedia_3_9
DEFAULT_TPF_DATASET = 'dbpedia_3_9'

DEFAULT_SPARQL_EP = 'http://172.16.9.15:8890/sparql' # "http://dbpedia.org/sparql" "http://172.16.9.15:8890/sparql"

MODE_TE_SPARQL = 'SPARQL'
MODE_TE_TPF = 'TPF'

#==================================================
#==================================================

class Endpoint:
    def __init__(self, service, cacheType = '', cacheDir = '.') :
        self.engine = None
        self.timeOut = None
        self.service = service
        self.reLimit = re.compile(r'limit\s*\d+',re.IGNORECASE)
        self.setCacheDir(cacheDir)
        self.mp_manager = mp.Manager()
        self.cache = self.mp_manager.dict()
        self.cacheType = cacheType
        self.do_cache = False
        self.reSupCom=re.compile(r'#[^>].*$',re.IGNORECASE | re.MULTILINE)
    
    def setTimeOut(self,to):
    	self.timeOut = to

    def getTimeOut(self):
        return self.timeOut

    def loadCache(self):
        cacheName = self.cacheDir+"/be4dbp-"+self.cacheType+".csv"
        if os.path.isfile(cacheName) :
            logging.info('Reading cache file : %s' % cacheName)
            with open(cacheName,"r", encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.cache[row['qhash']] = (row['ok'] == "True", row['wf'] == "True")

    def saveCache(self):
        if self.do_cache:
            cacheName = self.cacheDir+"/be4dbp-"+self.cacheType+".csv"
            logging.info('Writing cache file : %s' % cacheName)
            with open(cacheName,"w", encoding='utf-8') as f:
                fn=['ok','wf', 'qhash']
                writer = csv.DictWriter(f,fieldnames=fn)
                writer.writeheader()
                for x in iter(self.cache.keys()):
                    (ok,wellFormed) = self.cache[x]
                    writer.writerow({'ok':ok, 'wf':wellFormed,'qhash':x}) 

    def setCacheDir(self,cacheDir):
        self.cacheDir = cacheDir

    def caching(self, mode = True):
        if mode:
            self.cache = self.mp_manager.dict()
            self.loadCache()
        else:
            self.saveCache()
            self.cache.clear()
        self.do_cache = mode;

    def query(self, qstr, params = ''):
        return []

    def is_answering(self, qstr):
        '''Test if the query replies at least one answer (first value) and if the query is well formed (second value)'''
        raise EndpointException("There is no defined endpoint")
        return (False, EP_QueryWellFormed)

    def hash(self,qstr):
        return hashlib.sha512(qstr.encode('utf-8')).hexdigest()

    def setLimit1(self,query):
        if self.reLimit.search(query):
            nquery = self.reLimit.sub('limit 1',query)
        else:
            nquery = query + ' limit 1 '
        return nquery

    def notEmpty(self,query):
        #On cherche d'abord dans le cache
        qhash = self.hash(query) 
        if qhash in self.cache.keys():
            return self.cache[qhash]
        else:
            try:
                (ok,wf) = self.is_answering(self.setLimit1(query))
                self.cache[qhash] = (ok,wf)
                #---
                assert not(ok==True and wf==False), 'Bad response of is_answering'
                #---
                return (ok,wf)
            except EndpointException as e:
                logging.info('Erreur EndpointException : %s',e)
                raise Exception('Endpoint error',e)

#==================================================

class SPARQLEP (Endpoint): 
    def __init__(self, service = DEFAULT_SPARQL_EP, cacheDir = '.'):
        Endpoint.__init__(self, service = service, cacheType=MODE_TE_SPARQL, cacheDir=cacheDir)
        self.engine = SPARQLWrapper(self.service)
        self.engine.setReturnFormat(JSON)
        # self.sparql.setRequestMethod(POST)
        self.reVirtuosoTimeout = re.compile(r'Virtuoso 42000 Error The estimated execution time \d+ (sec) exceeds the limit of \d+ (sec).')
        self.reURLTimeout = re.compile(r"timed out")

    def setTimeOut(self,to):
    	Endpoint.setTimeOut(self,to)
    	self.engine.setTimeout(self.timeOut)

    def query(self, qstr, params = ''):
        self.engine.setQuery(qstr)
        return self.engine.query().convert()

    def is_answering(self, qstr):
        try:
            #print('Search in %s for %s'%(self.service,qstr.replace("\n", " ")))
            results = self.query(qstr)
            nb = len(results["results"]["bindings"])
            return (nb > 0, EP_QueryWellFormed)
        except QueryBadFormed as e:
            #logging.info('Erreur QueryBadFormed : %s',e)
            #print('QueryBadFormed',qstr)
            return (False, EP_QueryBadFormed)
        except EndPointNotFound as e:
            logging.info('Erreur EndPointNotFound : %s',e)
            #print('EndPointNotFound',qstr)
            raise EndpointException("SPARQL endpoint error (EndPointNotFound)",e,qstr)
        except EndPointInternalError as e:
            if self.reVirtuosoTimeout.search(e.__str__()):
                logging.info('Erreur timeout : %s',e)
                raise EndpointException("SPARQL timeout (TimeoutExpired)",e,qstr)
            else:
                logging.info('Erreur EndPointInternalError : %s',e)
                #print('EndPointInternalError',qstr)
                raise EndpointException("SPARQL endpoint error (EndPointInternalError)",e,qstr)
        except socket.timeout as e:
            logging.info('Erreur timeout : %s',e)
            #print('EndPointNotFound',qstr)
            raise EndpointException("socket.timeout (TimeoutExpired)",e,qstr)
        except Exception as e:
            if self.reURLTimeout.search(e.__str__()):
                logging.info('Erreur timeout : %s',e)
                raise EndpointException("URL timeout (TimeoutExpired)",e,qstr)
            else:
                logging.info('Erreur SPARQL EP ??? : %s',e)
                #print('Erreur SPARQL EP ??? :',e,qstr)
                raise EndpointException("SPARQL endpoint error (???)",e,qstr)

#==================================================

class DBPediaEP (SPARQLEP):
    def __init__(self, service = "http://dbpedia.org/sparql", cacheDir = '.'):
        SPARQLEP.__init__(self, service = service, cacheDir=cacheDir)

#==================================================

class TPFEP(Endpoint):
    def __init__(self,service = DEFAULT_TPF_EP, dataset = DEFAULT_TPF_DATASET, clientParams = '', cacheDir = '.'):
        Endpoint.__init__(self,service, cacheType=MODE_TE_TPF, cacheDir=cacheDir)
        self.dataset = dataset
        self.reSyntaxError = re.compile(r'\A(ERROR\:).*?\n\n(Syntax\ error\ in\ query).*',re.IGNORECASE)
        self.reQueryNotSupported = re.compile(r'\A(ERROR\:).*?\n\n(The\ query\ is\ not\ yet\ supported).*',re.IGNORECASE)
        self.appli = 'ldf-client'
        self.clientParams = clientParams

    def setEngine(self,en):
        self.appli = en

    def setDataset(self,d):
        self.dataset = d

    def query(self, qstr, params = ''):
        try:
            appli = self.appli # 'http_proxy= '+self.appli
            # 'run' n'existe que depuis python 3.5 !!! donc pas en 3.2 !!!!
            print('Execute:',appli,self.service+'/'+self.dataset,qstr, self.clientParams+' '+params)
            if (self.clientParams == '') and (params == ''):
                ret = subprocess.run([appli,self.service+'/'+self.dataset, qstr], 
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=self.timeOut)#, encoding='utf-8')
            else :
                ret = subprocess.run([appli,self.service+'/'+self.dataset, self.clientParams+params, qstr], 
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=self.timeOut)#, encoding='utf-8')
        except subprocess.CalledProcessError as e :
            raise TPFClientError( "TPF endpoint error (subprocess CalledProcessError) : "+e.__str__() )
        except subprocess.TimeoutExpired as e : # uniquement python 3.3 !!!
            raise TimeOut("TPF timeout (subprocess TimeoutExpired) : "+e.__str__())

        # pprint(ret)
        #sys.exit()
        out = ret.stdout
        if out != '':
            if type(out) != str : 
                out = out.decode('UTF-8')
            try:
                js = json.loads(out)
                # print(js)
                return js
            except json.JSONDecodeError as e: #Fonctionne pas en python 3.2... que depuis 3.5 !!!!
                # raise TPFClientError( "TPF endpoint error (JSONDecodeError) : %s"%str(e) )  
                # print("Pb conversion JSON")  
                return out            
        else:
            err = ret.stderr
            #print('##'+err+'##')
            #pprint(self.reSyntaxError.search(err).group(0))
            #pprint(self.reQueryNotSupported.search(err).group(0))
            #sys.exit()
            if self.reSyntaxError.search(err) != None: #ret.stderr.startswith('ERROR: Query execution could not start.\n\nSyntax error in query'):
                raise QueryBadFormed("QueryBadFormed : TPF client - %s"%err) # Exception('QueryBadFormed : %s' % err)
            elif self.reQueryNotSupported.search(err) != None: #ret.stderr.startswith('ERROR: Query execution could not start.\n\The query is not yet supported'):
                raise QueryBadFormed("QueryBadFormed : TPF client - %s"%err) # Exception('QueryBadFormed : %s' % err) 
            else:
                raise EndpointException('TPF Client error : %s' % err)
        
        # out = subprocess.check_output(['ldf-client',self.service+'/'+self.dataset, qstr.encode('utf8')]) #, encoding='utf-8') # ,stderr=subprocess.DEVNULL : python3.3 # timeout... uniquement python 3.3
        # #print('out=',out)
        # if out != '':
        #     return json.loads(out.decode('utf8'))
        # else: raise Exception('QueryBadFormed') #return []

    def is_answering(self, qstr):
        try:
            results = self.query(qstr)
            nb = len(results)
            return (nb > 0,EP_QueryWellFormed)
        except TPFClientError as e :
            logging.info('Erreur TPF Client : %s',e)
            #print('CalledProcessError',e)
            raise e
            #return (False,EP_QueryBadFormed)
        except TimeOut as e :
            logging.info('Erreur TimeoutExpired : %s',e)
            #print('TimeoutExpired',e)
            raise e
            #return (False,EP_QueryBadFormed)       
        except QueryBadFormed as e:
            #print('QueryBadFormed',qstr)
            return (False,EP_QueryBadFormed)
        except EndpointException as e:
            logging.info('Erreur TPF EP ??? : %s',e)
            raise e
        except Exception as e:
            logging.info('Erreur TPF EP ??? : %s',e)
            #print('Erreur TPF EP ??? :',e , qstr)
            raise EndpointException("TPF endpoint error (???)",e,qstr)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    logging.basicConfig(
        format='%(levelname)s:%(asctime)s:%(message)s',
        filename='scan.log',
        filemode='w',
        level=logging.DEBUG)

    print('main')

    ref = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?label
        WHERE { <http://dbpedia.org/resource/Asturias> rdfs:label ?label }
        LIMIT 10
    """

    pb = """
    select DISTINCT ?zzzzzz where{  ?x ?y ?zzzzzz FILTER regex(?zzzzzz, <http://dbpedia.org/class/yago/PresidentsOfTheUnitedState>)} LIMIT 5 
    """
    # sp = DBPediaEP()
    # sp.setTimeOut(0)
    # #sp.caching(True)
    # try:
    #   print(sp.notEmpty(ref))
    #   #sp.caching(False)
    # except Exception as e:
    #   print(e)

    q5 = """
    prefix : <http://www.example.org/lift2#> select ?s ?o where {?s :p3 "titi" . ?s :p1 ?o . ?s :p4 "tata"}
    """

    q6 = """
    prefix : <http://www.example.org/lift2#>  #njvbjonbtrg

    #Q2
    select ?s ?o where {
      ?s :p2 "toto" . #kjgfjgj
      # ?s ?p ?o .
      #?s <http://machin.org/toto#bidule> ?o ## jhjhj
    } limit 10 offset 0
    """
    print('origin:',ref)
    # http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
    sp = TPFEP(service = 'http://localhost:5001/dbpedia_3_9')
    sp.setEngine('/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')

    #sp.caching(True)
    try:
      print('NotEmpty:',sp.notEmpty(ref))
      #sp.saveCache()
    except Exception as e:
      #print(e)
    	pass