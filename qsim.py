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

from tools.Socket import SocketClient, MsgProcessor, SocketServer

from scipy.stats import uniform

import os.path

import unicodedata

#==================================================
class QueryCollectorMsgProcessor(MsgProcessor):
    def __init__(self,out_queue,ctx):
        super(QueryCollectorMsgProcessor, self).__init__()
        self.out_queue = out_queue
        self.ctx = ctx

    def processIn(self,mesg) :
        inQuery = json.loads(mesg)
        path = inQuery['path']
        # print('[QueryCollector] %s '%path)
        try: 

            if path == "inform" :
                data = inQuery['data']
                qID = inQuery['no']

                # print('Receiving request (%s):'%qID,data)
                with self.ctx.lck :
                    d = self.ctx.queryFeedback[qID]
                    d['p'] = data['p']
                    d['r'] = data['r']
                    d['treated'] = True
                    self.ctx.queryFeedback[qID] = d
            else :
                pass

        except Exception as e:
            print('[QueryCollector] ','Exception',e)
            print('[QueryCollector] ','About:',inQuery)



def processQuery(out_queue, ctx):
    print('[processQuery] Started ')
    server = SocketServer(host=ctx.host,port=ctx.port,ServerMsgProcessor = QueryCollectorMsgProcessor(out_queue,ctx) )
    server.run2()
    # out_queue.put(None)
    print('[processQuery] Stopped')

#==================================================

class Context(object):
    """docstring for Context"""

    def __init__(self):
        super(Context, self).__init__()
        self.manager = mp.Manager()
        self.host = '0.0.0.0'
        self.port = 5002
        self.sweep_ip = '127.0.0.1'
        self.sweep_port = 5003
        self.tpfc = TPFEP(service='http://localhost:5001/lift')
        self.tpfc.setEngine('/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client')
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

        self.queryFeedback = self.manager.dict()
        self.lck = mp.Lock()
        self.resQueue = mp.Queue()
        self.queryProcess = mp.Process(target=processQuery, args=(self.resQueue, self))
        self.queryProcess.start()

    def setSWEEPServer(self, host, port):
        self.sweep_ip = host
        self.sweep_port = port

    def setTPFClient(self, tpfc):
        self.tpfc = tpfc

    def stop(self):
        self.queryProcess.terminate()
        self.queryProcess.join()

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

def compactTime(base,distrib) : 
    print(distrib)
    delta = distrib[0]-base
    # on place le 1er à 0
    loc = [x-delta for x in distrib]
    # on recherche les écarts trop importants (2*gap) et on réduit
    for i in range(len(loc)-1):
        if loc[i+1]>loc[i]+ctx.gap*2 : 
            # print('trop long!')
            j = i+1
            d = loc[i+1]-loc[i]-(ctx.gap*2)
            while j<len(loc):
                loc[j] = loc[j]-d
                j +=1
    return loc

def timeDispatcher(entryList, ctx,nbq,period) :
    (_,_, firstTime) = entryList[0]
    (_,_, lastTime) = entryList[nbq-1]

    deltaTime = lastTime-firstTime

    # distrib = distributeUniform(ctx,firstTime,period,nbq)
    # print([d.isoformat() for d in distrib])
    distrib = compactTime(firstTime,distributeUniformRandom(ctx,firstTime,period,nbq))
    print(distrib)

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
    print('\n\n @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ \n Playing: %s' % file)
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
            p = mp.Process(target=run, args=(compute_queue,result_queue, ctx, dataset,file))
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
        sumT = dt.timedelta(minutes=0)
        nbOk = 0
        nbGap = 0
        for r in result :
            (n,no,t,m,v) = r
            sumT = sumT + t
            print('=========> Query %s : %s / %s'%(no,m,v), ' in ',t, 'second(s)')
            if m in ("Query ok", "Empty Query"): 
                nbOk += 1
            elif "TOGAP" in m :
                nbGap +=1
        print('The end for %s' % file)
        if len(result)>0: 
            avgT = sumT/len(result)
            print('Average processing',avgT )
        else:
            avgT = dt.timedelta(minutes=0)
        print('%d queries in gap'%nbOk)
        print('%d queries out of gap'%nbGap)
        return (len(result),avgT, nbOk, nbGap)
    else:
        return (0,dt.timedelta(minutes=0),0,0)

def toStr(s,p,o):
    return serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)

def run(inq, outq, ctx, datasource,file):
    sp = ctx.listeSP[datasource]
    qm = ctx.qm
    doPR = ctx.doPR
    gap = ctx.gap

    mss = inq.get()

    while mss is not None:
        (nbe,noq,query, bgp_list,d, oldDate,ip,valid) = mss

        query = unicodedata.normalize("NFKD", query)

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
        processing =  dt.timedelta(minutes=0)

        try: # (TPF|SPARQL|EmptyTPF|EmptySPARQL|QBFTPF|QBFSPARQL|TOTPF|TOSPARQL|NotTested)

            for i in range(TPF_CLIENT_REDO): # We try the query TPF_CLIENT_REDO times beause of TPF Client problems 
                try:

                    # mess = '<query time="'+date2str(dt.datetime.now())+'" no="'+no+'"><![CDATA['+query+']]></query>'
                    mess = '#bgp-list#'+quote_plus(bgp_list)+'\n'
                    mess += '#ipdate#'+ip+'@'+date2str(oldDate)+'\n'
                    qID = file+'#'+str(nbe) #+'@'+ip
                    mess += '#qID#'+qID+'\n'
                    mess += '#host#'+ctx.host+'\n'
                    mess += '#port#'+str(ctx.port)+'\n'
                    mess += query

                    ctx.queryFeedback[qID] = {'file':file, 'no':nbe,'query':'...', 'treated' : False, 'p':0.0, 'r':0.0, 'inGap':True, 'empty':False, 'duration': 0.0 }
                    # print(mess)

                    before = now()
                    rep = sp.query(mess)
                    after = now()
                    processing = after - before
                    # print('(%d)'%nbe,':',rep)
                    inGap = True
                    empty = False
                    if processing > gap:
                        messTO = '!!!!!!!!! hors Gap (%s) !!!!!!!!!'%gap.total_seconds()
                        code = ' TOGAP'
                        inGap = False
                    else : 
                        messTO = ''
                        code = ''
                    if rep == []:
                        messEmpty = " Empty query !!! "+messTO
                        empty = True
                        print('(%d, %s sec., %s)'%(nbe,processing.total_seconds(),valid),messEmpty)
                        client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                        data={'path': 'inform' ,'data': mess, 'errtype': 'Empty', 'no': no}
                        client.sendMsg2(data)
                        outq.put( (nbe, noq, processing, "Empty Query"+code,valid)  )
                    else: 
                        messOk = ': [...] '+ messTO
                        print('(%d, %s sec., %s)'%(nbe,processing.total_seconds(),valid),messOk)#,rep)
                        outq.put( (nbe, noq, processing, "Query ok"+code,valid)  )
                    with ctx.lck :
                        d = ctx.queryFeedback[qID]
                        d['duration'] = processing.total_seconds()
                        d['inGap'] = inGap
                        d['empty'] = empty
                        ctx.queryFeedback[qID] = d
                    break

                except TPFClientError as e :
                    print('(%d)'%nbe,'Exception TPFClientError (%d) : %s'%(i+1,e.__str__()))
                    if doPR:
                        client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                        data={'path': 'inform' ,'data': mess, 'errtype': 'CltErr', 'no': no}
                        client.sendMsg2(data)
                        print('(%d)'%nbe,'Request cancelled : ') 
                    if i>TPF_CLIENT_REDO/2:
                        time.sleep(TPF_CLIENT_TEMPO)

                except TimeOut as e :
                    print('(%d)'%nbe,'Timeout (%d) :'%(i+1),e)
                    if doPR:
                        client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                        data={'path': 'inform' ,'data': mess, 'errtype': 'TO', 'no': no}
                        client.sendMsg2(data)
                        print('(%d)'%nbe,'Request cancelled : ')  
                    if i>TPF_CLIENT_REDO/2:
                        time.sleep(TPF_CLIENT_TEMPO)


        except QueryBadFormed as e:
            print('(%d)'%nbe,'Query Bad Formed :',e)
            outq.put( (nbe, noq, processing, "QBFTPF",valid)  )
            if doPR:
                client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                data={'path': 'inform' ,'data': mess, 'errtype': 'QBF', 'no': no}
                client.sendMsg2(data)                
                print('(%d)'%nbe,'Request cancelled : ') 
        except EndpointException as e:
            print('(%d)'%nbe,'Endpoint Exception :',e)
            if doPR:
                client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                data={'path': 'inform' ,'data': mess, 'errtype': 'EQ', 'no': no}
                client.sendMsg2(data)                                
                print('(%d)'%nbe,'Request cancelled : ') 
        except Exception as e:
            print('(%d)'%nbe,'Exception execution query... :',e)
            if doPR:
                client = SocketClient(host = ctx.sweep_ip, port = ctx.sweep_port, ClientMsgProcessor = MsgProcessor() )
                data={'path': 'inform' ,'data': mess, 'errtype': 'Other', 'no': no}
                client.sendMsg2(data)                
                print('(%d)'%nbe,'Request cancelled : ')

        mss = inq.get()

    outq.put(None)


def loadDatabases(configFile, atpfServer, atpfClient) :
    XMLparser = etree.XMLParser(recover=True, strip_cdata=True)
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
        sp = TPFEP(service=atpfServer, dataset=f.get('nom'), clientParams=['-s xxxxx'])
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

#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linked Data Query simulator (for a modified TPF server)')
    parser.add_argument('files', metavar='file', nargs='+', help='files to analyse')

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
        pass

    cfg = ConfigParser(interpolation=ExtendedInterpolation())
    r = cfg.read(args.cfg)
    if r == []:
        print('Config file unkown')
        exit()
    print(cfg.sections())
    qsimCfg = cfg['QSIM-WS']
    atpfServer = qsimCfg['TPFServer']
    atpfClient = qsimCfg['TPFClient']
    agap = float(qsimCfg['Gap'])
    avalid = qsimCfg.getboolean('Precision-Recall')
    ato = float(qsimCfg['TimeOut'])
    ahost = qsimCfg['LocalIP']
    aport = qsimCfg['QSIM']

    sweepCfg = cfg['SWEEP']
    asweep = sweepCfg['LocalIP']
    ctx.ports = { 'DataCollector' : int(sweepCfg['DataCollector']), 'QueryCollector' : int(sweepCfg['QueryCollector']), 'DashboardEntry' : int(sweepCfg['DashboardEntry']) }
    ctx.sweep_host = sweepCfg['LocalIP']

    ctx.setSWEEPServer(ctx.sweep_host,ctx.ports['QueryCollector'])
    # http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
    # http://localhost:5001/dbpedia_3_9 server dppedia si : ssh -L 5001:172.16.9.3:5001 desmontils@172.16.9.15
    ctx.gap = dt.timedelta(minutes=agap)

    loadDatabases('config.xml', atpfServer, atpfClient)

    if avalid:
        ctx.doPR = True
    try:

        print('Running qsim on ', ahost+':'+aport)
        print('Start simulating with %d processes'%args.nb_processes)

        file_set = args.files
        nb_users = 0
        if (len(file_set)==1) and os.path.isdir(file_set[0]) :
            directory = file_set[0]
            XMLparser = etree.XMLParser(recover=True, strip_cdata=True)
            users_file = directory+'/users.xml'
            if existFile(users_file): 
                users = etree.parse(users_file, XMLparser)
                user = args.user
                file_set = []
                print('On directory:',directory)

                if user =='' :
                    print('For all users with avgBGPSize > 1')
                else:
                    print('For user:',user)

                for u in users.getroot():
                    if ( (user=='') and ( float(u.get('avgBGPSize')) > 1.0 ) and ( int(u.get('nbok')) > 0 )) \
                       or (u.get('ip') == user) :
                        print('+ Adding ',u.get('ip'))
                        nb_users +=1
                        for t in u :
                            if t.get('nb') != "0" : 
                                if user=='':
                                    f = directory+ date2filename(t.get('t')) + '/' + u.get('ip') + '-be4dbp-tested-TPF.xml'
                                else : 
                                    f = directory+ date2filename(t.get('t')) + '/' + user + '-be4dbp-tested-TPF.xml'
                                if existFile(f): file_set.append( f )
                        if u.get('ip') == user : break;
        

        print('For %d user(s), playing files :'%nb_users)
        print(file_set)
        
        sumT =  dt.timedelta(minutes=0)
        nb = 0
        pbGap = 0
        for file in file_set:
            if existFile(file):
                (nbq, avgT, noOk, nbGap) = play(file, ctx, args.nb_processes, args.dataset, args.nbq, args.offset, args.doEmpty, dt.timedelta(minutes=args.period)  )
                if avgT is not None :
                    sumT = sumT + avgT
                    pbGap += nbGap
                    nb += 1
                time.sleep(ctx.gap.total_seconds())

        # On attend d'avoir tous les résultats
        all = False
        while not all:
            all = True
            for (id,t) in ctx.queryFeedback.items() :
                if not(t['treated']) : 
                    all = False
                    time.sleep(3)
                    break;

    except KeyboardInterrupt:
        pass
    finally:
        print('\n\n To conclude :')


        ctx.stop()
        print('Process data :')
        nb = 0
        sump = 0
        sumr = 0
        for (id,t) in ctx.queryFeedback.items() :
            print('\t * ',id,':',t)
            nb +=1
            sump += t['p']
            sumr += t['r']
        print('Process stat :')
        print('\t - Queries out of gap: ',pbGap)
        if nb>0 :
            print('\t - Avg processing: ',sumT/nb)
            print('\t - nb queries : ',nb)
            print('\t - Avg p : ', sump/nb)
            print('\t - Avg r : ',sumr/nb)
        else: print('\t no queries treated')

        sweep = SocketClient(host=ctx.sweep_ip, port=ctx.ports['DashboardEntry'], msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
        res = sweep.sendMsg( { 'path' : '/run'} )
        nb = res[2]
        if nb>0:
            avgPrecision = res[3]/nb
            avgRecall = res[4]/nb
        else:
            avgPrecision = 0
            avgRecall = 0
        if (res[6]+res[0])>0:
            gn = res[5]/(res[6]+res[0])
        else: gn = 0
        print('server stat :')
        print('\t - Nb Queries :', nb)
        print('\t - Nb BGP : ', res[7])
        print('\t - Nb TPQ : ', res[9])
        print('\t - Avg p : ',avgPrecision)
        print('\t - Avg r : ',avgRecall)
        print('\t - GN : ',gn)

        print('The End!')
