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

from threading import *
import multiprocessing as mp
import iso8601 # https://pypi.python.org/pypi/iso8601/     http://pyiso8601.readthedocs.io/en/latest/
import time
from tools.ProcessSet import *
from tools.Stat import *
import json


import datetime as dt
import argparse
from tools.Endpoint import TPFEP, TPFClientError, TimeOut, QueryBadFormed, EndpointException
from tools.tools import now, date2str, date2filename, existFile

from lib.QueryManager import QueryManager
from lib.bgp import serializeBGP2str

from flask import Flask, render_template, request, jsonify
# http://flask.pocoo.org/docs/0.12/
# from flask_cas import CAS, login_required

from lxml import etree  # http://lxml.de/index.html#documentation

import requests as http
# http://docs.python-requests.org/en/master/user/quickstart/

from urllib.parse import urlparse, quote_plus
from configparser import ConfigParser, ExtendedInterpolation

from scipy.stats import uniform

import os.path

class Context(object):
    """docstring for Context"""

    def __init__(self):
        super(Context, self).__init__()
        self.sweep = 'http://127.0.0.1:5002'
        self.tpfc = TPFEP(service='http://localhost:5000/lift')
        self.tpfc.setEngine(
            '/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')
        self.tree = None
        self.debug = False
        self.listeNoms = None
        self.listeBases = dict()
        self.listeSP = dict()
        self.version = '1.0'
        self.name = 'Name'
        self.ok = True
        self.nbQuery = 0
        self.qm = QueryManager(modeStat=False)
        self.doPR = False
        self.lastProcessing = dt.timedelta() #-1
        self.gap = 60
        self.addr = ''
        self.addr_ext = ''
        # self.BGPRefList = dict()
        # self.BGPNb = 0

    def setLDQPServer(self, host):
        self.sweep = host

    def setTPFClient(self, tpfc):
        self.tpfc = tpfc


ctx = Context()
randomU = uniform()

#==================================================	


TPF_SERVEUR = 'http://127.0.0.1:5000'
TPF_CLIENT = '/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client'
SWEEP_SERVEUR = 'http://127.0.0.1:5002'

TPF_SERVEUR_DATASET = 'lift' # default dataset

TPF_CLIENT_REDO = 3 # Number of client execution in case of fails
TPF_CLIENT_TEMPO = 0.01 # sleep duration if client fails

#==================================================

def distributeUniform(ctx, base, period, nbq) :
    delta = min(1.5*ctx.gap,period / nbq)
    print(delta)
    return [base+(delta*n) for n in range(nbq)]

def distributeUniformRandom(ctx, base, period, nbq) :
    l = randomU.rvs(size=nbq)
    l.sort()
    return [base+(period*x) for x in l]

def compactTime(base,distrib) : # TODO : limiter la taille des intervals vides à 1.5*gap
    delta = distrib[0]-base
    return [x-delta for x in distrib]

def timeDispatcher(entryList, ctx,nbq,period) :
    (_,_, firstTime) = entryList[0]
    (_,_, lastTime) = entryList[nbq-1]

    deltaTime = lastTime-firstTime

    # distrib = distributeUniform(ctx,firstTime,period,nbq)
    # print([d.isoformat() for d in distrib])
    distrib = compactTime(firstTime,distributeUniformRandom(ctx,firstTime,period,nbq))

    if (nbq>1) and (deltaTime == dt.timedelta(minutes=0)) :
        
        print('For %d queries, time interval is :'%nbq)
        print('\t',[d.isoformat() for d in distrib])
        n = 0
        for (no,e,date) in entryList:
            ndate = distrib[n]
            n += 1
            e.set('datetime',ndate.isoformat())
            print('\t',n, ' - ', ndate)
    else: 
        # print('ok')
        pass
    return (firstTime,lastTime)


def play(file,ctx,nb_processes, dataset, nbq,offset, doEmpty, period):
    compute_queue = mp.Queue(nb_processes)
    result_queue = mp.Queue()
    print('Traitement de %s' % file)
    parser = etree.XMLParser(recover=True, strip_cdata=True)
    tree = etree.parse(file, parser)
    #---
    #dtd = etree.DTD('http://documents.ls2n.fr/be4dbp/log.dtd')
    #assert dtd.validate(tree), '%s non valide au chargement : %s' % (
    #    file, dtd.error_log.filter_from_errors()[0])
    #---
    #print('DTD valide !')

    n = 0 # nombre d'entries vues
    date = 'no-date'
    ip = 'ip-'+tree.getroot().get('ip').split('-')[0]

    # validQueries in (TPF|SPARQL|EmptyTPF|EmptySPARQL|QBFTPF|QBFSPARQL|TOTPF|TOSPARQL|NotTested) with "NotTested" by default
    validQueries = ['TPF','NotTested']
    if doEmpty: 
        validQueries.append('EmptyTPF')
        print('Empty queries computed')
    else : 
        print('Empty queries suppressed')

    nbEntries = 0
    n = 0
    entryList = []
    for e in tree.findall('entry') :
        if ((nbq==0) and (n>=offset)) or ((nbEntries<nbq) and (n>=offset)):
            if e.get("valid") in validQueries :
                entryList.append( (n,e, fromISO(e.get('datetime')) )  )
                nbEntries += 1
                # print(e.get("valid"))
        n += 1
        if (nbq>0) and (n>offset+nbq): break

    nbq = nbEntries

    print(len(entryList), ' valid entries')
    print(nbq, ' entries to treat')

    if (nbq>0) and (nbEntries > 0) : 

        (firstTime,lastTime) = timeDispatcher(entryList,ctx,nbq,period)

        current_date = dt.datetime.now()
        processes = []
        for i in range(nb_processes):
            p = mp.Process(target=run, args=(compute_queue,result_queue, ctx, dataset))
            processes.append(p)
        for p in processes:
            p.start()

        result = []

        n = 0
        for (no,entry,oldDate) in  entryList:
            n += 1
            date = current_date+(fromISO(entry.get('datetime'))-firstTime)

            print('(%d) new entry to add (%s)- executed at %s' % (n,entry.get("valid"),date))
            rep = ''
            for x in entry :
                if x.tag == 'bgp':
                    if len(x)>0:
                        rep += etree.tostring(x).decode('utf-8')
            compute_queue.put( (n, no, entry.find('request').text, rep, date, oldDate, ip, entry.get("valid") )  ) 
  
            if nbq>0 and n >= nbq : break

        
        for p in processes:
            compute_queue.put(None)

        nbNone = 0
        while nbNone <nb_processes:
            inq = result_queue.get()
            if inq is None:
                nbNone += 1
            else:
                result.append(inq)

        for p in processes:
            p.join()

        time.sleep(ctx.gap.total_seconds())
        print('---')
        print('%d queries treated on %d queries'%(nbq,nbEntries) )
        for r in result :
            (n,no,m,v) = r
            print('=========> Query %s : %s / %s'%(no,m,v))
        print('Fin de traitement de %s' % file)
        time.sleep(ctx.gap.total_seconds())

def toStr(s,p,o):
    return serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)

def run(inq, outq, ctx, datasource):
    sp = ctx.listeSP[datasource]
    qm = ctx.qm
    doPR = ctx.doPR
    gap = ctx.gap
    sweep = ctx.sweep

    mss = inq.get()

    while mss is not None:
        (nbe,noq,query, bgp_list,d, oldDate,ip,valid) = mss
        duration = max(dt.timedelta.resolution, d-dt.datetime.now())
        print('(%d)'%nbe,'Sleep:',duration.total_seconds(),' second(s)')
        time.sleep(duration.total_seconds())

        try:
            if bgp_list=='':
                (bgp,nquery) = qm.extractBGP(query)
                query = nquery
                bgp_list = serializeBGP2str(bgp)
            # print(serializeBGP2str(bgp) )
        except Exception as e:
            print(e)
            pass

        print('(%d)'%nbe,'Query:',query)
        no = 'qsim-'+str(nbe)
        bgp_list = '<l>'+bgp_list+'</l>'
        print(bgp_list)

        try: # (TPF|SPARQL|EmptyTPF|EmptySPARQL|QBFTPF|QBFSPARQL|TOTPF|TOSPARQL|NotTested)

            for i in range(TPF_CLIENT_REDO): # We try the query TPF_CLIENT_REDO times beause of TPF Client problems 
                try:

                    # mess = '<query time="'+date2str(dt.datetime.now())+'" no="'+no+'"><![CDATA['+query+']]></query>'
                    mess = '#bgp-list#'+quote_plus(bgp_list)+'\n'+'#ipdate#'+ip+'@'+date2str(oldDate)+'\n'+query
                    if doPR:
                        url = ctx.sweep+'/query'
                        print('on:',url)
                        # try:
                        #     s = http.post(url,data={'data':mess, 'no':no, 'bgp_list': '<l>'+bgp_list+'</l>'})
                        #     print('(%d)'%nbe,'Request posted : ',s.json()['result'])
                        # except Exception as e:
                        #     print('Exception',e)

                    before = now()
                    rep = sp.query(mess)
                    after = now()
                    processing = after - before
                    # print('(%d)'%nbe,':',rep)
                    if rep == []:
                       print('(%d, %s sec., %s)'%(nbe,processing.total_seconds(),valid)," Empty query !!!")
                       url = sweep+'/inform'
                       s = http.post(url,data={'data':mess,'errtype':'Empty', 'no':no})
                       outq.put( (nbe, noq, "Empty Query",valid)  )
                    else: 
                        print('(%d, %s sec., %s)'%(nbe,processing.total_seconds(),valid),': [...]')#,rep)
                        outq.put( (nbe, noq, "Query ok",valid)  )
                    if processing > gap:
                        print('(%d, %s sec., %s)'%(nbe,processing.total_seconds(),valid),'!!!!!!!!! hors Gap (%s) !!!!!!!!!'%gap.total_seconds())
                        outq.put( (nbe, noq, "TOGAP",valid)  )
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
            outq.put( (nbe, noq, "QBFTPF",valid)  )
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

    outq.put(None)

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linked Data Query simulator (for a modified TPF server)')
    parser.add_argument('files', metavar='file', nargs='+', help='files to analyse')


    parser.add_argument("--sweep", default=SWEEP_SERVEUR, dest="sweep",help="SWEEP ('"+str(SWEEP_SERVEUR)+"' by default)")
    parser.add_argument("-s", "--server", default=TPF_SERVEUR,dest="tpfServer", help="TPF Server ('"+TPF_SERVEUR+"' by default)")
    parser.add_argument("-c", "--client", default=TPF_CLIENT,dest="tpfClient", help="TPF Client ('...' by default)")
    parser.add_argument("-v", "--valid", dest="valid",action="store_true", help="Do precision/recall")
    parser.add_argument("-g", "--gap", type=float, default=60,dest="gap", help="Gap in minutes (60 by default)")
    parser.add_argument("-to", "--timeout", type=float, default=None, dest="timeout",help="TPF Client Time Out in minutes (no timeout by default).")
    parser.add_argument("--host", default="127.0.0.1",dest="host", help="host ('127.0.0.1' by default)")
    parser.add_argument("--port", type=int, default=5002,dest="port", help="Port (5002 by default)")

    parser.add_argument("-f", "--config", default='',dest="cfg", help="Config file")

    parser.add_argument("-d", "--dataset", default=TPF_SERVEUR_DATASET, dest="dataset", help="TPF Server Dataset ('"+TPF_SERVEUR_DATASET+"' by default)")
    parser.add_argument("-p", "--proc", type=int, default=mp.cpu_count(), dest="nb_processes",help="Number of processes used (%d by default)" % mp.cpu_count())
    parser.add_argument('-n',"--nbQueries", type=int, default=0, dest="nbq", help="Max queries to study (0 by default, i.e. all queries)")
    parser.add_argument('-o',"--offset", type=int, default=0, dest="offset", help="first query to study (0 by default, i.e. all queries)")

    parser.add_argument("--empty", dest="doEmpty",action="store_true", help="Also run empty queries")

    parser.add_argument("-i", type=float, default=60, dest="period", help="Period in minutes (60 by default)")

    parser.add_argument("-u", "--user", default='',dest="user", help="In case of directory, play for this user (attempt a 'users.xml' file in the dir.")

    args = parser.parse_args()

    if (args.cfg == ''):
        asweep = args.sweep
        atpfServer = args.tpfServer
        atpfClient = args.tpfClient
        ahost = args.host
        aport = args.port
        agap = args.gap
        avalid = args.valid
        ato = args.timeout
        aurl = 'http://'+ahost+":"+str(aport)
        aurl_ext = aurl
    else:
        cfg = ConfigParser(interpolation=ExtendedInterpolation())
        r = cfg.read(args.cfg)
        if r == []:
            print('Config file unkown')
            exit()
        print(cfg.sections())
        qsimCfg = cfg['QSIM-WS']
        asweep = qsimCfg['SWEEP']
        atpfServer = qsimCfg['TPFServer']
        atpfClient = qsimCfg['TPFClient']
        agap = float(qsimCfg['Gap'])
        avalid = qsimCfg.getboolean('Precision-Recall')
        ato = float(qsimCfg['TimeOut'])
        aurl = qsimCfg['LocalAddr']
        purl = urlparse(aurl)
        ahost = purl.hostname
        aport = purl.port
        aurl_ext = qsimCfg['ExternalAddr']

    ctx.setLDQPServer(asweep)
    # http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
    # http://localhost:5001/dbpedia_3_9 server dppedia si : ssh -L 5001:172.16.9.3:5001 desmontils@172.16.9.15
    ctx.gap = dt.timedelta(minutes=agap)
    XMLparser = etree.XMLParser(recover=True, strip_cdata=True)
    configFile = 'config.xml'
    ctx.tree = etree.parse(configFile, XMLparser)
    #---
    dtd = etree.DTD('config.dtd')
    assert dtd.validate(ctx.tree), '%s non valide au chargement : %s' % (
        configFile, dtd.error_log.filter_from_errors()[0])
    #---
    lb = ctx.tree.getroot().findall('listeBases/base_de_donnee')
    for l in lb:
        f = l.find('fichier')
        ref = l.find('référence')
        if ref.text is None:
            ref.text = ''
        print('Configure ', l.get('nom'), ' in ', atpfServer+'/'+f.get('nom'))
        sp = TPFEP(service=atpfServer, dataset=f.get('nom'), clientParams=['-s %s' % asweep])
        sp.setEngine(atpfClient)
        #if ato: sp.setTimeout(ato)
        ctx.listeBases[l.get('nom')] = {'fichier': f.get('nom'), 'prefixe': f.get('prefixe'), 'référence': ref.text,
                                        'description': etree.tostring(l.find('description'), encoding='utf8').decode('utf8'),
                                        'tables': []}
        ctx.listeSP[l.get('nom')] = sp
    ctx.listeNoms = list(ctx.listeBases.keys())
    ctx.version = ctx.tree.getroot().get('version')
    ctx.name = ctx.tree.getroot().get('name')
    if ctx.tree.getroot().get('debug') == 'false':
        ctx.debug = False
    else:
        ctx.debug = True
    if avalid:
        ctx.doPR = True
    try:
        ctx.addr = aurl
        ctx.addr_ext = aurl_ext
        print('Running qsim ')

        print('Start simulating with %d processes'%args.nb_processes)


        file_set = args.files
        
        if (len(file_set)==1) and os.path.isdir(file_set[0]) :
            directory = file_set[0]
            users_file = directory+'/users.xml'
            if existFile(users_file): 
                users = etree.parse(users_file, XMLparser)
                user = args.user
                file_set = []
                print('On directory:',directory)

                if user =='' :
                    print('For all users')
                else:
                    print('For user:',user)

                for u in users.getroot():
                    if (user=='') or (u.get('ip') == user) :
                        for t in u :
                            if t.get('nb') != "0": 
                                if user=='':
                                    f = directory+ date2filename(t.get('t')) + '/' + u.get('ip') + '-be4dbp-tested-TPF.xml'
                                else : 
                                    f = directory+ date2filename(t.get('t')) + '/' + user + '-be4dbp-tested-TPF.xml'
                                if existFile(f): file_set.append( f )
                        if u.get('ip') == user : break;

        print('Playing files :')
        print(file_set)
        
        for file in file_set:
            if existFile(file):
                play(file, ctx, args.nb_processes, args.dataset, args.nbq, args.offset, args.doEmpty, dt.timedelta(minutes=args.period)  )

    except KeyboardInterrupt:
        pass
    finally:
        # ctx.qm.stop()
        pass
    print('Fin')










