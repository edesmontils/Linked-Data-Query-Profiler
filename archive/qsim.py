#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Application ...
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

# import sys
# from tools.Socket import *
from threading import *
import multiprocessing as mp

# import multiprocessing as mp

import datetime as dt
import iso8601 # https://pypi.python.org/pypi/iso8601/     http://pyiso8601.readthedocs.io/en/latest/
import time

from tools.tools import *
from tools.Endpoint import *
from tools.ProcessSet import *
from tools.Stat import *

import json

import requests as http
# http://docs.python-requests.org/en/master/user/quickstart/

# import re
import argparse

from lxml import etree  # http://lxml.de/index.html#documentation
# from lib.bgp import *
from lib.QueryManager import *

#==================================================	


TPF_SERVEUR = 'http://127.0.0.1:5000'
TPF_SERVEUR_DATASET = 'lift' # default dataset
TPF_CLIENT = '/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client'
TPF_CLIENT_REDO = 3 # Number of client execution in case of fails
TPF_CLIENT_TEMPO = 0.01 # sleep duration if client fails
SWEEP_SERVEUR = 'http://127.0.0.1:5002'

#==================================================

def play(file,nb_processes, server,client,timeout, dataset, nbq,offset,doValid, sweep, gap):
    compute_queue = mp.Queue(nb_processes)
    print('Traitement de %s' % file)
    parser = etree.XMLParser(recover=True, strip_cdata=True)
    tree = etree.parse(file, parser)
    #---
    dtd = etree.DTD('http://documents.ls2n.fr/be4dbp/log.dtd')
    assert dtd.validate(tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---
    print('DTD valide !')

    nbe = 0 # nombre d'entries traitées
    n = 0 # nombre d'entries vues
    date = 'no-date'
    ip = 'ip-'+tree.getroot().get('ip').split('-')[0]
    for entry in tree.getroot():
        if entry.tag == 'entry':
            n += 1
            if n>=offset:
                nbe += 1
                if nbe == 1:
                    date_ref = fromISO(entry.get('datetime'))
                    current_date = dt.datetime.now()
                    processes = []
                    for i in range(nb_processes):
                        p = mp.Process(target=run, args=(compute_queue, server, client, timeout, dataset, sweep,doValid,gap))
                        processes.append(p)
                    for p in processes:
                        p.start()
                date = fromISO(entry.get('datetime'))
                ide = entry.get('logline')
                valid = entry.get("valid")
                if valid is not None :
                    if valid in ['TPF','EmptyTPF'] :
                        date = current_date+(date-date_ref)
                        print('(%d) new entry to add - executed at %s' % (n,date))
                        rep = ''
                        for x in entry :
                            if x.tag == 'bgp':
                                if len(x)>0:
                                    rep += etree.tostring(x).decode('utf-8')
                        # print(rep)
                        compute_queue.put( (n, entry.find('request').text, rep, date )  ) 
                    else: print('(%d) entry not executed : %s' % (n,valid))
                else: print('(%d) entry not executed (not validated)' % n)   
                if nbq>0 and nbe >= nbq : break
        else:
            pass

    if nbe>0: 
        for p in processes:
            compute_queue.put(None)
        for p in processes:
            p.join()

def toStr(s,p,o):
    return serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)

def run(inq, server, client, timeout, dataset, sweep, doPR, gap):
    sp = TPFEP(service = server, dataset = dataset)#, clientParams= '-s '+host+':'+str(port)  ) #'http://localhost:5000/lift') 
    sp.setEngine(client) #'/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')
    qm = QueryManager(modeStat = False)
    if timeout: sp.setTimeout(timeout)
    mss = inq.get()
    while mss is not None:
        (nbe,query, bgp_list,d) = mss
        duration = max(dt.timedelta.resolution, d-dt.datetime.now())
        print('(%d)'%nbe,'Sleep:',duration.total_seconds(),' second(s)')
        time.sleep(duration.total_seconds())

        try:
            (bgp,nquery) = qm.extractBGP(query)
            query = nquery
            if bgp_list=='':
                bgp_list = serializeBGP2str(bgp)
            # print(serializeBGP2str(bgp) )
        except Exception as e:
            print(e)
            pass

        print('(%d)'%nbe,'Query:',query)
        no = 'qsim-'+str(nbe)
        print(bgp_list)

        try:
            for i in range(TPF_CLIENT_REDO): # We try the query TPF_CLIENT_REDO times beause of TPF Client problems 
                try:

                    mess = '<query time="'+date2str(dt.datetime.now())+'" no="'+no+'"><![CDATA['+query+']]></query>'
                    if doPR:
                        url = sweep+'/query'
                        print('on:',url)
                        try:
                            s = http.post(url,data={'data':mess, 'no':no, 'bgp_list': '<l>'+bgp_list+'</l>'})
                            print('(%d)'%nbe,'Request posted : ',s.json()['result'])
                        except Exception as e:
                            print('Exception',e)

                    before = now()
                    rep = sp.query(query)
                    after = now()
                    processing = after - before
                    # print('(%d)'%nbe,':',rep)
                    if rep == []:
                       print('(%d, %s sec.)'%(nbe,processing.total_seconds())," Empty query !!!")
                       url = sweep+'/inform'
                       s = http.post(url,data={'data':mess,'errtype':'Empty', 'no':no})
                    else: 
                        print('(%d, %s sec.)'%(nbe,processing.total_seconds()),': [...]')#,rep)
                    if processing > gap :
                        print('(%d, %s sec.)'%(nbe,processing.total_seconds()),'!!!!!!!!! hors Gap (%s) !!!!!!!!!'%gap.total_seconds())
                    break

                except TPFClientError as e :
                    print('(%d)'%nbe,'Exception TPFClientError (%d) : %s'%(i+1,e.__str__()))
                    if doPR:
                        url = sweep+'/inform'
                        s = http.post(url,data={'data':mess,'errtype':'CltErr', 'no':no})
                        print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
                    if i>TPF_CLIENT_REDO/2:
                        time.sleep(TPF_CLIENT_TEMPO)

                except TimeOut as e :
                    print('(%d)'%nbe,'Timeout (%d) :'%(i+1),e)
                    if doPR:
                        url = sweep+'/inform'
                        s = http.post(url,data={'data':mess,'errtype':'TO', 'no':no})
                        print('(%d)'%nbe,'Request cancelled : ',s.json()['result'])  
                    if i>TPF_CLIENT_REDO/2:
                        time.sleep(TPF_CLIENT_TEMPO)


        except QueryBadFormed as e:
            print('(%d)'%nbe,'Query Bad Formed :',e)
            if doPR:
                url = sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'QBF', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
        except EndpointException as e:
            print('(%d)'%nbe,'Endpoint Exception :',e)
            if doPR:
                url = sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'EQ', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
        except Exception as e:
            print('(%d)'%nbe,'Exception execution query... :',e)
            if doPR:
                url = sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'Other', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result'])

        mss = inq.get()


#==================================================
#==================================================
#==================================================

parser = argparse.ArgumentParser(description='Linked Data Query simulator (for a modified TPF server)')
parser.add_argument('files', metavar='file', nargs='+', help='files to analyse')
parser.add_argument("--sweep", default=SWEEP_SERVEUR, dest="sweep", help="SWEEP ('"+str(SWEEP_SERVEUR)+"' by default)")
parser.add_argument("-s","--server", default=TPF_SERVEUR, dest="tpfServer", help="TPF Server ('"+TPF_SERVEUR+"' by default)")
parser.add_argument("-d", "--dataset", default=TPF_SERVEUR_DATASET, dest="dataset", help="TPF Server Dataset ('"+TPF_SERVEUR_DATASET+"' by default)")
parser.add_argument("-c", "--client", default=TPF_CLIENT, dest="tpfClient", help="TPF Client ('...' by default)")
parser.add_argument("-v", "--valid", dest="valid", action="store_true", help="Do precision/recall")
parser.add_argument("-to", "--timeout", type=float, default=None, dest="timeout",help="TPF Client Time Out in minutes (no timeout by default).")
parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",
                    help="Number of processes used (%d by default)" % mp.cpu_count())
parser.add_argument('-n',"--nbQueries", type=int, default=0, dest="nbq", help="Max queries to study (0 by default, i.e. all queries)")
parser.add_argument('-o',"--offset", type=int, default=0, dest="offset", help="first query to study (0 by default, i.e. all queries)")
parser.add_argument("-g", "--gap", type=float, default=60, dest="gap", help="Gap in minutes (60 by default)")

args = parser.parse_args()

# http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
# http://localhost:5001/dbpedia_3_9 server dppedia si : ssh -L 5001:172.16.9.3:5001 desmontils@172.16.9.15
print('Start simulating with %d processes'%args.nb_processes)
file_set = args.files
for file in file_set:
    if existFile(file):
    	play(file,args.nb_processes, args.tpfServer, args.tpfClient, args.timeout, args.dataset, args.nbq, args.offset, args.valid, args.sweep, dt.timedelta(minutes= args.gap)  )
