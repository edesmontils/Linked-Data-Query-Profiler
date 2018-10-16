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

from queue import Empty
import multiprocessing as mp

import datetime as dt
import time

import csv
from tools.tools import now, fromISO, existFile, date2str

from io import StringIO

from tools.ssa import *

from rdflib import Variable, URIRef, Literal

from lxml import etree  # http://lxml.de/index.html#documentation
from lib.bgp import serialize2string, egal, calcPrecisionRecall, canonicalize_sparql_bgp, serializeBGP, unSerialize, simplifyVars, unSerializeBGP
from lib.QueryManager import QueryManager

from collections import OrderedDict

from functools import reduce

import json

from tools.Socket import SocketServer, MsgProcessor, SocketClient
import argparse

from configparser import ConfigParser, ExtendedInterpolation
from urllib.parse import urlparse, unquote_plus
from operator import itemgetter
#==================================================

SWEEP_IN_ENTRY = 1
SWEEP_IN_DATA = 2
SWEEP_IN_END = 3

SWEEP_IN_LOG = 7

SWEEP_IN_QUERY = 4
SWEEP_OUT_QUERY = 5
SWEEP_IN_BGP = 6

SWEEP_ALL_BGP = False

SWEEP_PURGE = -3

SWEEP_ENTRY_TIMEOUT = 0.8  # percentage of the gap
SWEEP_PURGE_TIMEOUT = 0.1  # percentage of the gap

SWEEP_DEBUG_BGP_BUILD = False
SWEEP_DEBUB_PR = False

#==================================================


def toStr(s, p, o):
    return serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)

def listToStr(l) :
    s = [serialize2string(s) for s in l]
    return s

def mapValues(di,i,j) :
    if (di != i) and isinstance(j, Variable):
        return i
    else:
        return None 

def hashBGP(bgp):
    rep = ''
    for (s,p,o) in bgp:
            rep += toStr(s,p,o)+' . '
    return hash(rep)

#==================================================


class TripplePattern(object):
    """docstring for TripplePattern"""

    def __init__(self, s, p, o):
        super(TripplePattern, self).__init__()
        (self.s, self.p, self.o) = (s, p, o)

    def equal(self, tp):
        return egal(self.spo(), tp.spo())

    def toStr(self):
        return serialize2string(self.s)+' '+serialize2string(self.p)+' '+serialize2string(self.o)

    def toString(self):
        return self.toStr()

    def spo(self):
        return (self.s, self.p, self.o)


class TriplePatternQuery(TripplePattern):
    """docstring for TriplePatternQuery"""

    def __init__(self, s, p, o, time, client, sm, pm, om):
        super(TriplePatternQuery, self).__init__(s, p, o)
        (self.time, self.client, self.sm, self.pm,
         self.om) = (time, client, sm, pm, om)
        # self.su = set()
        # self.pu = set()
        # self.ou = set()

    def isDump(self):  # tp is <?s ?p ?o> ?
        return isinstance(self.s, Variable) and isinstance(self.p, Variable) and isinstance(self.o, Variable)

    def renameVars(self, i):
        if isinstance(self.s, Variable):
            self.s = Variable("s"+str(i).replace("-", "_"))
        if isinstance(self.p, Variable):
            self.p = Variable("p"+str(i).replace("-", "_"))
        if isinstance(self.o, Variable):
            self.o = Variable("o"+str(i).replace("-", "_"))

    def isTriple(self):
        return not(isinstance(self.s, Variable) or isinstance(self.p, Variable) or isinstance(self.o, Variable))

    def sign(self):
        if isinstance(self.s, Variable):
            s = Variable("s")
        else:
            s = self.s
        if isinstance(self.p, Variable):
            p = Variable("p")
        else:
            p = self.p
        if isinstance(self.o, Variable):
            o = Variable("o")
        else:
            o = self.o
        return hash(toStr(s, p, o))

    def shape(self) :
        sh = ''
        if isinstance(self.s, Variable):
            sh += 'V'
        elif isinstance(self.s, URIRef):
            sh += 'I'
        else: sh += 'C'
        if isinstance(self.p, Variable):
            sh += 'V'
        elif isinstance(self.p, URIRef):
            sh += 'I'
        else: sh += 'C'
        if isinstance(self.o, Variable):
            sh += 'V'
        elif isinstance(self.o, URIRef):
            sh += 'I'
        else: sh += 'C'     
        return sh

    # hypothèse : pas de jointure prédicat
    def iriJoinable(self) :
        return self.shape() in ["IVC", "IVI", "IIV", "IVV", "VII", "VVI"]

    def nestedLoopOf2(self,baseTPQ):
        # 1:sp->sp 2:so->so 3:s->s 4:so->os 5:s->o 6:o->s 8:po->po 9:p->p 10:sp->po
        # 0/1/2/3 ->(0/1/2/3,0/1/2/3,0/1/2/3)
        # HEURISTIQUE : on donne la priorité au motif en étoile sur le sujet !
        base = (None,baseTPQ.s,baseTPQ.p,baseTPQ.o)
        if  self.s in baseTPQ.sm: 
            if self.p in baseTPQ.pm: nl = ( 2 , (1,2,0) ) # sp->sp
            elif self.o in baseTPQ.om: nl = ( 2 , (1,0,3) ) # so->so
            else: nl = ( 1 , (1,0,0) ) # s->s
        elif self.s in baseTPQ.om:
            if self.o in baseTPQ.sm: nl = ( 2 , (3,0,1) ) # so->os
            elif self.p in baseTPQ.pm: nl = ( 2 , (3,2,0) ) # sp->po
            else: nl = ( 1 , (3,0,0) ) # s->o
        elif self.o in baseTPQ.sm: nl = ( 1 , (0,0,1) ) # o->s
        elif self.p in baseTPQ.pm:
            if self.o in baseTPQ.om: nl = ( 2 , (0,2,3) ) # po->po
            else: nl = ( 1 , (0,2,0) ) # p->p
        else: nl = ( 0 , (0,0,0) )
        (couv,injection) = nl
        # print('injection:',injection)
        return ( couv , 
                { 's':doInjection(base,injection[0],self.s),  
                  'p':doInjection(base,injection[1],self.p),
                  'o':doInjection(base,injection[2],self.o) } )

def doInjection(base, injection, val) :
    if injection > 0 : return base[injection]
    else: return val

#==================================================



class BasicGraphPattern:
    def __init__(self, gap=None, tpq=None):
        self.tp_set = []
        self.input_set = set()  # ens. des hash des entrées, pour ne pas mettre 2 fois la même
        if gap is None:
            self.gap = dt.timedelta(minutes=1)
        else:
            self.gap = gap
        if tpq is None:
            self.birthTime = now()
            self.time = now()
            self.client = 'Unknown'
        else:
            self.birthTime = tpq.time
            self.time = tpq.time
            self.client = tpq.client
            self.add(tpq)

    def add(self, tpq, sgn=None):
        assert isinstance(tpq, TriplePatternQuery), "BasicGraphPattern.Add : Pb type TPQ"
        assert (self.client == 'Unknown') or (self.client == tpq.client), "BasicGraphPattern.Add : client différent"
        assert tpq.time - self.time <= self.gap, "BasicGraphPattern.Add : TPQ pas dans le gap"
        if self.client == 'Unknown':
            self.client = tpq.client
            self.birthTime = tpq.time
        self.time = tpq.time
        self.tp_set.append(tpq)
        if sgn is None:
            self.input_set.add(tpq.sign())
        else:
            self.input_set.add(sgn)

    def update(self, tp, ntpq):
        if SWEEP_DEBUG_BGP_BUILD:
            print('\t Déjà présent avec ', toStr(tp.s, tp.p, tp.o))
            print('\t MàJ des mappings')
            print(  '\t\t ', listToStr(tp.sm), '+', listToStr(ntpq.sm), 
                    '\n\t\t ', listToStr(tp.pm), '+', listToStr(ntpq.pm), 
                    '\n\t\t ', listToStr(tp.om), '+', listToStr(ntpq.om) )
        tp.sm.update(ntpq.sm)
        tp.pm.update(ntpq.pm)
        tp.om.update(ntpq.om)
        self.input_set.add(ntpq.sign())  # ATTENTION : Utile ?

    def age(self):
        return now() - self.time

    def isOld(self):
        return self.age() > self.gap

    def toString(self):
        rep = ''
        for tpq in self.tp_set:
            rep += tpq.toStr() + " .\n "
        return rep

    def print(self, tab=''):
        print(tab, 'BGP:', self.client, ' at ', self.time)
        print(tab, self.toString())

    def canBeCandidate(self, tpq):
        assert isinstance(
            tpq, TriplePatternQuery), "BasicGraphPattern.canBeCandidate : Pb type TPQ"
        return (tpq.client == self.client) and (tpq.time - self.time <= self.gap) and (tpq.sign() not in self.input_set)

    def findIRIJoin(self,ntpq) :
        # "IVC", "IVI", "IIV", "IVV", "VII", "VVI"
        if ntpq.iriJoinable() :
            trouve = False
            ntpqIriSet = set()
            sh = ntpq.shape()
            if sh[0]=='I':
                ntpqIriSet.add(ntpq.s)
            if sh[2]=='I':
                ntpqIriSet.add(ntpq.o)
            for (_, tpq) in enumerate(self.tp_set):
                if tpq.iriJoinable():
                    tpqIriSet = set()
                    sh = tpq.shape()
                    if sh[0]=='I':
                        tpqIriSet.add(tpq.s)
                    if sh[2]=='I':
                        tpqIriSet.add(tpq.o)
                    if ntpqIriSet & tpqIriSet :
                        trouve = True
                        break;

            if trouve : return (True,ntpq,tpq, (None,None,None))
            else : return (False,None,None,None)
        else: return (False,None,None,None)

    def findNestedLoop(self, ntpq):
        assert isinstance(
            ntpq, TriplePatternQuery), "BasicGraphPattern.findTP : Pb type TPQ"
        ref_couv = 0
        trouve = False
        fromTP = None
        candTP = None
        mapVal = (None, None, None)
        # on regarde si une constante du sujet et ou de l'objet est une injection 
        # provenant d'un tpq existant (par son résultat)
        for (_, tpq) in enumerate(self.tp_set):
            if SWEEP_DEBUG_BGP_BUILD:
                print('_____', '\n\t\t Comparaison de :',
                      ntpq.toStr(), '\n\t\t avec le TP :', tpq.toStr())
                print('\t\tbsm:', listToStr(tpq.sm), 
                      '\n\t\tbpm:', listToStr(tpq.pm), 
                      '\n\t\tbom:', listToStr(tpq.om) )
                # print('\t\tbsm:', listToStr(tpq.sm), '\n\t\t\tbsu:', listToStr(tpq.su),
                #       '\n\t\tbpm:', listToStr(tpq.pm), '\n\t\t\tbpu:', listToStr(tpq.pu),
                #       '\n\t\tbom:', listToStr(tpq.om), '\n\t\t\tbou:', listToStr(tpq.ou))
            (couv, d) = ntpq.nestedLoopOf2(tpq)
            #couv : nombre de mappings trouvés (hypothèse de double injection)
            #d : indique les "constantes" de ntpq qui font l'objet d'injection 
            #    par tpq (et la variable de celui-ci)

            if (couv > ref_couv):
                # on prend les cas où il y a le plus grand nombre d'injections. 
                # doutes sur le fait qu'il peut y en avoir plusieurs...
                # on calcule le TPQ possible
                ctp = TriplePatternQuery(d['s'], d['p'], d['o'], ntpq.time, ntpq.client, ntpq.sm, ntpq.pm, ntpq.om)
                (inTP, _) = ctp.equal(tpq)
                if not(inTP):
                    trouve = True
                    ref_couv = couv
                    fromTP = tpq
                    candTP = ctp
                    mapVal = mapValues(d['s'],ntpq.s,tpq.s), mapValues(d['p'],ntpq.p,tpq.p), mapValues(d['o'],ntpq.o,tpq.o)
                    break
        # end for tpq

        return (trouve, candTP,fromTP, mapVal)

    def existTP(self, candtp,fromTP):
        inTP = False
        tp = None
        # peut-être que un TP similaire a déjà été utilisé pour une autre valeur... 
        # alors pas la peine de le doubler
        for tp in self.tp_set:
            (inTP, _) = candtp.equal(tp)
            if inTP:
                (inTP2, _) = fromTP.equal(tp)
                if not(inTP2):
                    break
                else:
                    inTP = False
        return (inTP, tp)

#==================================================

class DataCollectorMsgProcessor(MsgProcessor):
    def __init__(self,out_queue,ctx):
        super(DataCollectorMsgProcessor, self).__init__()
        self.out_queue = out_queue
        self.entry_id = 0
        self.ctx = ctx

    def processIn(self,mesg) :
        inLog = json.loads(mesg)
        self.ctx.nbEntries.value += 1

        time = fromISO(inLog['time'])

        # client = inLog['ip']
        # if client is None:
        #     client = self.cl_ip
        # elif client in ["undefined","", "undefine"]:
        #     client = self.cl_ip
        # elif "::ffff:" in client:
        #     client = client[7:]
        client = 'Batman'

        data = inLog['data']
        try:
            tree = etree.parse(StringIO(data), self.ctx.parser)
            # nbEntries += 1
            entry = None
            eid = self.entry_id
            self.entry_id += 1

            (s,p,o,t,c,sm,pm,om) = (None,None,None,time,client,set(),set(),set())

            for e in tree.getroot():
                if e.tag == 'e':
                    if e[0].get('type')=='var' : e[0].set('val','s')
                    if e[1].get('type')=='var' : e[1].set('val','p')
                    if e[2].get('type')=='var' : e[2].set('val','o')
                    s = unSerialize(e[0])
                    p = unSerialize(e[1])
                    o = unSerialize(e[2])
                    print('[DataCollector] in : ',toStr(s,p,o))
                elif e.tag == 'd':
                    if isinstance(s,Variable): sm.add(unSerialize(e[0]))
                    if isinstance(p,Variable): pm.add(unSerialize(e[1]))
                    if isinstance(o,Variable): om.add(unSerialize(e[2]))

                elif e.tag == 'm':
                    # s = unSerialize(e[0])
                    # p = unSerialize(e[1])
                    # o = unSerialize(e[2])
                    # print('new meta : ',toStr(s,p,o))
                    pass
                else:
                    pass
        except Exception as e:
            print('[DataCollector] ','Exception',e)
            print('[DataCollector] ','About:',data)
        else: self.out_queue.put((eid,  (s,p,o,t,c,sm,pm,om)  ))

def processDataCollector(out_queue, ctx):
    print('[processDataCollector] Started ')
    server = SocketServer(port=ctx.ports['DataCollector'],ServerMsgProcessor = DataCollectorMsgProcessor(out_queue,ctx) )
    server.run2()
    out_queue.put(None)
    print('[processDataCollector] Stopped')

#==================================================
class QueryCollectorMsgProcessor(MsgProcessor):
    def __init__(self,out_queue,ctx):
        super(QueryCollectorMsgProcessor, self).__init__()
        self.out_queue = out_queue
        self.entry_id = 0
        self.ctx = ctx

    def processIn(self,mesg) :
        inQuery = json.loads(mesg)
        self.entry_id += 1
        path = inQuery['path']
        print('[QueryCollector] %s '%path)
        if path == "put" :
            data = inQuery['data']
            queryID = inQuery['no']

            # print('Receiving request:',data)
            try:
                tree = etree.parse(StringIO(data), self.ctx.parser)
                q = tree.getroot()

                client = q.get('client')
                # if client is None:
                #     q.set('client',str(self.cl_ip) )
                # elif client in ["undefined","", "undefine"]:
                #     q.set('client',str(self.cl_ip) )
                # elif "::ffff:" in client:
                #     q.set('client', client[7:])
                # else : q.set('client',client)
                q.set('client','Batman')

                print('[QueryCollector] QUERY - ip-remote:',self.cl_ip,' client:',client, ' choix:',q.get('client'))
                ip = q.get('client')

                query = q.text.strip()
                time = fromISO(q.attrib['time']) 
                print('@ ', time)
                # print('fromISO ', fromISO(q.attrib['time']) )
                print('now', now())

                if query.startswith('#bgp-list#') :
                    t = query.split('\n')
                    bgp_list = unquote_plus(t[0][10:])
                    del t[0]
                    queryCode = t[0][8:]
                    del t[0]
                    if t[0].startswith('#qID#') :
                        clientQueryID = t[0][5:]
                        del t[0]
                        clientHost = t[0][6:]
                        del t[0]
                        clientPort = t[0][6:]
                        del t[0]
                        queryInfo = (clientQueryID, clientHost,int(clientPort))
                    else:
                        queryInfo = ()
                    query = '\n'.join(t)
                else:
                    bgp_list = '<l/>'
                    queryCode = ip
                    queryInfo = ()

                l = []
                print('[QueryCollector] ---',queryCode, '|', queryInfo ,'---')
                print('[QueryCollector] ',query)
                print('[QueryCollector] ',bgp_list)
                lbgp = etree.parse(StringIO(bgp_list), self.ctx.parser)
                for x in lbgp.getroot():
                    bgp = unSerializeBGP(x)
                    l.append(bgp)

                if len(l) == 0:
                    print('[QueryCollector] ','BGP list empty... extracting BGP from the query')
                    (bgp,nquery) = self.ctx.qm.extractBGP(query)
                    query = nquery
                    l.append(bgp)


                self.ctx.nbQueries.value += len(l)
                if queryID =='ldf-client':
                    pass #queryID = queryID + str(ctx.nbQueries)
                print('[QueryCollector] ','ID',queryID)
                rang = 0
                for bgp in l :
                    rang += 1
                    with self.ctx.qId.get_lock():
                        self.ctx.qId.value += 1
                        qId = self.ctx.qId.value
                        if queryID is None:
                            queryID = 'id'+str(qId)
                        self.ctx.queryFeedback[qId] = queryInfo
                        # print(self.ctx.queryFeedback)
                    self.out_queue.put(
                        (SWEEP_IN_QUERY, qId, (time, ip, query, bgp, str(queryID)+'_'+str(rang),queryCode)))

            except Exception as e:
                print('[QueryCollector] ','Exception',e)
                print('[QueryCollector] ','About:',data)

        elif path == "del" :
            x = inQuery['x ']
            print('[QueryCollector] ','del')
            self.out_queue.put((SWEEP_OUT_QUERY, 0, x))

        elif path == 'inform' :
            inQuery = json.loads(mesg)
            #ip = request.remote_addr

            errtype = inQuery['errtype']
            queryNb = inQuery['no']
            print('[QueryCollector] ','inform %s / %s'%(errtype,queryNb))
            if errtype == 'QBF':
                print('[QueryCollector] ','(%s)'%queryNb,'Query Bad Formed :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbQBF.value += 1
            elif errtype == 'TO':
                print('[QueryCollector] ','(%s)'%queryNb,'Time Out :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbTO.value += 1
            elif errtype == 'CltErr':
                print('[QueryCollector] ','(%s)'%queryNb,'TPF Client Error for :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbClientError.value += 1
            elif errtype == 'EQ':
                print('[QueryCollector] ','(%s)'%queryNb,'Error Query for :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbEQ.value += 1
            elif errtype == 'Other':
                print('[QueryCollector] ','(%s)'%queryNb,'Unknown Pb for query :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbOther.value += 1
            elif errtype == 'Empty':
                print('[QueryCollector] ','(%s)'%queryNb,'Empty for :',inQuery['data'])
                self.ctx.nbEmpty.value += 1
            else:
                print('[QueryCollector] ','(%s)'%queryNb,'Unknown Pb for query :',inQuery['data'])
                # self.ctx.delQuery(queryNb)
                self.out_queue.put((SWEEP_OUT_QUERY, 0, queryNb))
                self.ctx.nbCancelledQueries.value += 1
                self.ctx.nbOther.value += 1

        else :
            pass

        
def processQueryCollector(out_queue, ctx):
    print('[processQueryCollector] Started ')
    server = SocketServer(port=ctx.ports['QueryCollector'],ServerMsgProcessor = QueryCollectorMsgProcessor(out_queue,ctx) )
    server.run2()
    out_queue.put(None)
    print('[processQueryCollector] Stopped')

#==================================================

def processBGPDiscover(in_queue, val_queue, ctx):
    gap = ctx.gap
    purge_timeout = (ctx.gap*SWEEP_PURGE_TIMEOUT).total_seconds()
    BGP_list = []
    print('[processBGPDiscover] Started')
    try:

        entry = in_queue.get()

        while entry != None:
            (id, val) = entry
            if val == SWEEP_PURGE:
                # print('[BGPDiscover] Purge (%d waiting BGPs)'%len(BGP_list))
                # val_queue.put((SWEEP_PURGE, 0, None))
                pass
            else:
                # print('[BGPDiscover] TPQ Analysys')
                (s, p, o, time, client, sm, pm, om) = val
                new_tpq = TriplePatternQuery(s, p, o, time, client, sm, pm, om)
                new_tpq.renameVars(id)

                if SWEEP_DEBUG_BGP_BUILD:
                    print(
                        '============================================== ',id,' ==============================================')
                    print('Etude de :', new_tpq.toStr())
                    print('|sm:', listToStr(new_tpq.sm), '\n|pm:',
                          listToStr(new_tpq.pm), '\n|om:', listToStr(new_tpq.om))

                if not(new_tpq.isDump()):
                    trouve = False
                    for (i, bgp) in enumerate(BGP_list):

                        if SWEEP_DEBUG_BGP_BUILD:
                            print('-----------------------------------\n\t Etude avec BGP ', i)
                            bgp.print('\t\t\t')

                        if bgp.canBeCandidate(new_tpq):
                            # Si c'est le même client, dans le gap et un TP identique,
                            #  n'a pas déjà été utilisé pour ce BGP

                            (trouve, candTP,fromTP,mapVal) = bgp.findNestedLoop(new_tpq)
                            if trouve:
                                # le nouveau TPQ pourrait être produit par un nested loop... on teste alors
                                # sa "forme d'origine" 'candTP'
                                if SWEEP_DEBUG_BGP_BUILD:
                                    print('\t\t ok avec :', new_tpq.toStr(),' sur ',mapVal,
                                          '\n\t\t |-> ', candTP.toStr())
                                (ok, tp) = bgp.existTP(candTP,fromTP)
                                if ok:
                                    # La forme existe déjà. Il faut ajouter les mappings !
                                    # mais il faut que ce ne soit pas celui qui a injecté !
                                    bgp.update(tp, new_tpq)
                                else:  # C'est un nouveau TPQ du BGP !
                                    if SWEEP_DEBUG_BGP_BUILD:
                                        print('\t\t Ajout de ', new_tpq.toStr(), '\n\t\t avec ', candTP.toStr())
                                    bgp.add(candTP, new_tpq.sign())
                                (vs,vp,vo) = mapVal
                                # if vs is not None: fromTP.su.add(vs)
                                # if vp is not None: fromTP.pu.add(vp)
                                # if vo is not None: fromTP.ou.add(vo)
                                if ctx.optimistic:
                                    bgp.time = time
                                break #on en a trouvé un bon... on arrête de chercher !

                            else : # on essaye la jointure sur les IRI
                                (trouve, candTP,fromTP,mapVal) = bgp.findIRIJoin(new_tpq)
                                if trouve :
                                    # le nouveau TPQ pourrait être produit par une jointure sur IRI... on teste alors
                                    # sa "forme d'origine" 'candTP'
                                    if SWEEP_DEBUG_BGP_BUILD:
                                        print('\t\t ok avec :', new_tpq.toStr(),' sur IRI',
                                              '\n\t\t |-> ', candTP.toStr())
                                    (ok, tp) = bgp.existTP(candTP,fromTP)
                                    if ok:
                                        trouve = False
                                    else:  # C'est un nouveau TPQ du BGP !
                                        if SWEEP_DEBUG_BGP_BUILD:
                                            print('\t\t Ajout de ', new_tpq.toStr(), '\n\t\t avec ', candTP.toStr())
                                        bgp.add(candTP, new_tpq.sign())
                                        if ctx.optimistic:
                                            bgp.time = time
                                        break #on en a trouvé un bon... on arrête de chercher !                                    

                        else:
                            if (new_tpq.client == bgp.client) and (new_tpq.time - bgp.time <= gap):
                                if SWEEP_DEBUG_BGP_BUILD:
                                    print('\t\t Déjà ajouté')
                                pass
                    # end "for BGP"

                    # pas trouvé => nouveau BGP
                    if not(trouve):
                        if SWEEP_DEBUG_BGP_BUILD:
                            print('\t Création de ', new_tpq.toStr(),
                                  '-> BGP ', len(BGP_list))
                        BGP_list.append(BasicGraphPattern(gap, new_tpq))

            # envoyer les trop vieux !
            old = []
            recent = []
            for bgp in BGP_list:
                # print(currentTime,bgp.time)
                if bgp.isOld():
                    old.append(bgp)
                else:
                    recent.append(bgp)
            for bgp in old:
                val_queue.put((SWEEP_IN_BGP, -1, bgp))
            BGP_list = recent
            ctx.nbBGP.value = len(BGP_list)

            try:
                entry = in_queue.get(timeout=purge_timeout )
            except Empty:
                entry = (0, SWEEP_PURGE)  

    except KeyboardInterrupt:
        # penser à purger les derniers BGP ou uniquement autoutr du get pour gérer fin de session
        pass
    finally:
        for bgp in BGP_list:
            val_queue.put((SWEEP_IN_BGP, -1, bgp))
        BGP_list.clear()
        ctx.nbBGP.value = 0
        # val_queue.put((SWEEP_PURGE, 0, None))
        val_queue.put(None)
    print('[processBGPDiscover] Stopped')

#==================================================


def testPrecisionRecallBGP(queryList, bgp, gap):
    # print('====================================')
    best = 0
    test = [(tp.s, tp.p, tp.o) for tp in bgp.tp_set]
    # print(test)
    best_precision = 0
    best_recall = 0
    for i in queryList:
        ((time, ip, query, qbgp, queryID,queryCode), old_bgp, precision, recall) = queryList[i]

        if SWEEP_DEBUB_PR:
            rep = ''
            for (s, p, o) in qbgp:
                rep += toStr(s, p, o)+' . \n'
            print('comparing with query (%s) : ' % queryID, rep)

        if (ip == bgp.client) and (bgp.birthTime >= time) and (bgp.birthTime - time <= gap):
            (precision2, recall2, _, _) = calcPrecisionRecall(qbgp, test)
            if precision2*recall2 > precision*recall:
            #if (precision2 > precision) or ((precision2 == precision) and (recall2 > recall)):
                if precision2*recall2 > best_precision*best_recall:
                #if (precision2 > best_precision) or ((precision2 == best_precision) and (recall2 > best_recall)):
                    best = i
                    best_precision = precision2
                    best_recall = recall2
    if best > 0:
        ((time, ip, query, qbgp, queryID,queryCode), old_bgp, precision, recall) = queryList[best]
        queryList[best] = ((time, ip, query, qbgp, queryID,queryCode), bgp, best_precision, best_recall)
        if SWEEP_DEBUB_PR:
            print('association:', queryID, best_precision, best_recall)
            bgp.print()
        # essayer de replacer le vieux...
        if old_bgp is not None:
            return testPrecisionRecallBGP(queryList, old_bgp, gap)
        else:
            return None
    else:
        return bgp

def processValidation(in_queue, memoryQueue, ctx):
    valGap = ctx.gap * 2
    gap = ctx.gap
    purge_timeout = (ctx.gap*SWEEP_PURGE_TIMEOUT).total_seconds()
    currentTime = now()
    queryList = OrderedDict()
    print('[processValidation] Started :\n\t- gap :',gap,'\n\t- valGap : ',valGap)
    try:
        inq = in_queue.get()

        while inq is not None:
            (mode, id, val) = inq

            if mode == SWEEP_IN_QUERY:
                # print('[processValidation] Query analysis')
                # ctx.stat['nbQueries'] += 1
                (time, ip, query, qbgp, queryID,queryCode) = val
                currentTime = now()
                if SWEEP_DEBUB_PR:
                    print('+++')
                    print(currentTime, ' New query', val)
                (precision, recall, bgp) = (0, 0, None)
                queryList[id] = ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall)
                # print('[processValidation] Query added')
                ctx.stat['goldenNumber'] += len(qbgp)

            elif mode == SWEEP_IN_BGP:
                bgp = val
                # print('[processValidation] BGP Analysis')
                if bgp is not None: # HEURISTIQUE : Un BGP qui contient un simple triplet n'est pas valide
                    ctx.stat['genBGP'] += 1
                    if (len(bgp.input_set) ==1) and bgp.tp_set[0].isTriple() :
                        # print("=================>     BGP not inserted")
                        # bgp.print()
                        pass
                    else:
                        ctx.stat['nbBGP'] += 1
                        currentTime = now()
                        if SWEEP_DEBUB_PR:
                            print('+++')
                            print(currentTime, ' New BGP')
                            val.print()
                        old_bgp = testPrecisionRecallBGP(queryList, bgp, gap)
                        if SWEEP_DEBUB_PR:
                            if old_bgp is not None:
                                print('BGP not associated and archieved :')
                                old_bgp.print()
                        if old_bgp is not None:
                            memoryQueue.put( (4,  (0, 'none', 'none', old_bgp.birthTime, old_bgp.client, None, None, old_bgp, 0, 0) ) )                      
                # print('[processValidation] BGP added')

            # dans le cas où le client TPF n'a pas pu exécuter la requête...
            elif mode == SWEEP_OUT_QUERY:
                # print('[processValidation] Query deletion')
                # suppress query 'queryID'
                for i in queryList:
                    ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall) = queryList[i]
                    if queryID == val:
                        if SWEEP_DEBUB_PR:
                            print('---')
                            print(currentTime, ' Deleting query', queryID)
                        queryList.pop(i)
                        # ctx.stat['nbQueries'] -= 1
                        if bgp is not None:
                            if SWEEP_DEBUB_PR:
                                print('-')
                                print('extract its BGP')
                                bgp.print()
                            old_bgp = testPrecisionRecallBGP(queryList, bgp, gap)
                            if old_bgp is not None:
                                memoryQueue.put( (4, (0, 'none', 'none', old_bgp.birthTime, old_bgp.client, None, None, old_bgp, 0, 0)) )
                        else:
                            if SWEEP_DEBUB_PR:
                                print('-')
                                print('No BGP to extract')
                        ctx.stat['goldenNumber'] -= len(qbgp)
                        break
                # print('[processValidation] Query deleted')

            else:  # mode == SWEEP_PURGE
                # print('[processValidation] Purge (%d waiting queries)'%len(queryList))
                currentTime = now()

            # Suppress older queries
            # print('[processValidation] Trying to suppress oldest queries (%d waiting queries)'%len(queryList))
            old = []
            for id in queryList:
                ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall) = queryList[id]
                if currentTime - time > valGap:
                    old.append(id)

            for id in old:
                ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall) = queryList.pop(id)
                if SWEEP_DEBUB_PR:
                    print('--- purge ', queryID, '(', time, ') ---', precision, '/', recall, '---', ' @ ', currentTime, '---')
                    print(query)
                    print('---')
                #---
                assert (bgp is None) or (ip == bgp.client), 'Client Query différent de client BGP'
                #---
                memoryQueue.put( (4, (id, queryID,queryCode, time, ip, query, qbgp, bgp, precision, recall)) )
                ctx.stat['sumRecall'] += recall
                ctx.stat['sumPrecision'] += precision
                ctx.stat['sumQuality'] += (recall+precision)/2
                ctx.stat['nbQueries'] += 1
                if bgp is not None:
                    if SWEEP_DEBUB_PR:
                        print(".\n".join( [tp.toStr() for tp in bgp.tp_set]) )
                    ctx.stat['sumSelectedBGP'] += 1
                else:
                    if SWEEP_DEBUB_PR:
                        print('Query not assigned')
                if SWEEP_DEBUB_PR:
                    print('--- --- @'+ip+' --- ---')
                    print(' ')
            ctx.nbREQ.value = len(queryList)

            try:
                inq = in_queue.get(timeout=purge_timeout )
            except Empty:
                inq = (SWEEP_PURGE, 0, None)  

    except KeyboardInterrupt:
        pass
    finally:
        for id in queryList:
            ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall) = queryList.pop(id)
            if SWEEP_DEBUB_PR:
                print('--- purge ', queryID, '(', time, ') ---',
                      precision, '/', recall, '---', ' @ ', currentTime, '---')
                print(query)
                print('---')
            #---
            assert (bgp is None) or (ip == bgp.client), 'Client Query différent de client BGP'
            #---
            memoryQueue.put( (4, (id, queryID,queryCode, time, ip, query, qbgp, bgp, precision, recall)) )
            ctx.stat['sumRecall'] += recall
            ctx.stat['sumPrecision'] += precision
            ctx.stat['sumQuality'] += (recall+precision)/2
            if bgp is not None:
                if SWEEP_DEBUB_PR:
                    print(".\n".join([tp.toStr() for tp in bgp.tp_set]))
                ctx.stat['sumSelectedBGP'] += 1
            else:
                if SWEEP_DEBUB_PR:
                    print('Query not assigned')
            if SWEEP_DEBUB_PR:
                print('--- --- @'+ip+' --- ---')
                print(' ')
        ctx.nbREQ.value = 0
    print('[processValidation] Stopped')

#==================================================

def addBGP2Rank(bgp, nquery, line, precision, recall, ranking):
    ok = False
    for (i, (t, d, n, query, ll, p, r)) in enumerate(ranking):
        if bgp == d:
            ok = True
            break
    if ok:
        ll.add(line)
        if query == None:
            query = nquery
        ranking[i] = (now(), d, n+1, query, ll, p+precision, r+recall)
    else:
        ranking.append((now(),bgp, 1, nquery, {line}, precision, recall))

def processMemory(ctx, duration, inQueue):
    sscBGP = SpaceSavingCounter(ctx.memSize)
    sscQueries  = SpaceSavingCounter(ctx.memSize)
    lastTimeMemorySaved = ctx.startTime
    nbMemoryChanges = 0
    maxNbMemory = 0
    maxRankingBGPs = 0
    maxRankingQueries = 0
    print('[processMemory] Started :\n\t- to : ',duration, '\n\t- mem. duration :',ctx.memDuration)
    try:
        while True:
            try:
                inq = inQueue.get(timeout= duration.total_seconds() )
            except Empty:
                inq = (0, True)           

            if inq is None:
                break
            else:
                (mode,mess) = inq

            if ((mode==0) and (nbMemoryChanges > 0)) or (nbMemoryChanges > 1000): # Save memory in a CSV file
                # print('[processMemory] Save (%d entries to save ; %d rankedBGPs ; %d rankedQueries ; %d in memory)'%(nbMemoryChanges,len(ctx.rankingBGPs),len(ctx.rankingQueries),len(ctx.memory) ) )
                ctx.saveMemory()
                ctx.saveUsers()
                nbMemoryChanges = 0

            if mode==4:
                # print('[processMemory] new Entry in memory (%d entries to save ; %d rankedBGPs ; %d rankedQueries ; %d in memory)'%(nbMemoryChanges,len(ctx.rankingBGPs),len(ctx.rankingQueries),len(ctx.memory) ) )
                (id, queryID,queryCode, time, ip, query, qbgp, bgp, precision, recall) = mess

                with ctx.lck:
                    ctx.memory.append( ( now() , id, queryID,queryCode, time, ip, query, bgp, precision, recall) )
                nbMemoryChanges += 1

                if query is not None :
                    sbgp = canonicalize_sparql_bgp(qbgp)
                    with ctx.lck: 
                        addBGP2Rank(sbgp, query, id, precision, recall, ctx.rankingQueries)
                    sscQueries.add(hashBGP(sbgp),sbgp)
                    (g,o,tpk) = sscQueries.queryTopK(ctx.memSize)
                    while len(ctx.topKQueries) > 0: ctx.topKQueries.pop(0)
                    with ctx.lck :
                        for e in tpk:
                            ctx.topKQueries.append(sscQueries.monitored[e])

                        queryInfo = ctx.queryFeedback[id]
                        if (queryInfo is not None) and (queryInfo != () ) :
                            (clientQueryID, clientHost,clientPort) = queryInfo
                            data={'path': 'inform' ,'data': {'p':precision, 'r':recall}, 'no': clientQueryID}
                            print('Envoie d''infos à ',clientHost,':',clientPort)
                            client = SocketClient(host = clientHost, port = clientPort, ClientMsgProcessor = MsgProcessor() )
                            client.sendMsg2(data)
                        else: print('Pas d''infos à retourner')

                if bgp is not None :
                    sbgp = canonicalize_sparql_bgp([(tp.s, tp.p, tp.o) for tp in bgp.tp_set])
                    if SWEEP_ALL_BGP or (len(sbgp) > 1):
                        with ctx.lck: 
                            addBGP2Rank(sbgp, query, id, 0, 0, ctx.rankingBGPs)
                        sscBGP.add(hashBGP(sbgp),sbgp)
                        (g,o,tpk) = sscBGP.queryTopK(ctx.memSize)
                        while len(ctx.topKBGPs) > 0: ctx.topKBGPs.pop(0)
                        with ctx.lck :
                            for e in tpk:
                                ctx.topKBGPs.append(sscBGP.monitored[e])

                if (query is not None) and (ip is not None) :
                    if queryCode in ctx.usersMemory.keys():
                        (nb, sumPrecision, sumRecall) = ctx.usersMemory[queryCode]
                    else:
                        (nb, sumPrecision, sumRecall) = (0,0,0)
                    ctx.usersMemory[queryCode] = (nb+1,sumPrecision+precision, sumRecall+recall)

            else:
                pass
                # print('[processMemory] Purge (%d entries to save ; %d rankedBGPs ; %d rankedQueries ; %d in memory)'%(nbMemoryChanges,len(ctx.rankingBGPs),len(ctx.rankingQueries),len(ctx.memory) ) )

            maxNbMemory = max(maxNbMemory, len(ctx.memory))
            maxRankingBGPs = max( maxRankingBGPs, len(ctx.rankingBGPs) ) 
            maxRankingQueries = max( maxRankingQueries, len(ctx.rankingQueries) )
            
            # Oldest elements in short memory are deleted
            threshold = now() - ctx.memDuration
            with ctx.lck:
                while len(ctx.memory)>0 :
                    (t, id, queryID, queryCode, time, ip, query, bgp, precision, recall) = ctx.memory[0]
                    if t < threshold : ctx.memory.pop(0)
                    else: break
                i = 0
                while i<len(ctx.rankingBGPs) :
                    (chgDate, d, n, query, ll, p, r) = ctx.rankingBGPs[i]
                    if chgDate < threshold : ctx.rankingBGPs.pop(i)
                    else: i += 1
                i = 0
                while i<len(ctx.rankingQueries) :
                    (chgDate, d, n, query, ll, p, r) = ctx.rankingQueries[i]
                    if chgDate < threshold : ctx.rankingQueries.pop(i)
                    else: i += 1

    except KeyboardInterrupt:
        pass
    finally :
        # ctx.saveMemory()
        # ctx.saveUsers()
        print('[processMemory] Stopped :\n\t- max memory size : ', maxNbMemory, '\n\t- max BGP ranking size : ',maxRankingBGPs ,'\n\t- max Queries ranking size : ',maxRankingQueries )

#==================================================

class SWEEP(MsgProcessor):  # Abstract Class
    def __init__(self, gap, to, opt, ports, mem = 100, mode = 0):
        super(SWEEP, self).__init__()
        #---
        assert isinstance(gap, dt.timedelta)
        #---
        self.gap = gap
        self.timeout = to
        self.optimistic = opt  # màj de la date du BGP avec le dernier TP reçu ?
        self.nlast = mem
        self.lck = mp.Lock()
        self.manager = mp.Manager()
        self.ports = ports
 
        self.memory = self.manager.list()
        self.lastTimeMemorySaved = now()
        self.topKBGPs = self.manager.list()
        self.topKQueries = self.manager.list()
        self.rankingBGPs = self.manager.list()
        self.rankingQueries = self.manager.list()
        self.startTime = now()
        self.memSize = mem # for long term memory (ssc)
        self.memDuration = 10*gap # for short term memory

        self.usersMemory = self.manager.dict()
        self.queryFeedback = self.manager.dict()

        self.nbBGP = mp.Value('i', 0)
        self.nbREQ = mp.Value('i', 0)
        self.nbEntries = mp.Value('i', 0)
        self.nbQueries = mp.Value('i', 0)

        self.entry_id = mp.Value('i', 0)

        self.nbCancelledQueries = mp.Value('i', 0)
        self.nbQBF = mp.Value('i', 0)
        self.nbTO = mp.Value('i', 0)
        self.nbEQ = mp.Value('i', 0)
        self.nbOther = mp.Value('i', 0)
        self.nbClientError = mp.Value('i', 0)

        self.nbEmpty = mp.Value('i', 0)

        self.qId = mp.Value('i', 0)

        self.stat = self.manager.dict({'sumRecall': 0, 'sumPrecision': 0, 'goldenNumber':0, 'genBGP':0,
                                  'sumQuality': 0, 'nbQueries': 0, 'nbBGP': 0, 'sumSelectedBGP': 0})

        self.entryQueue = mp.Queue()
        self.validationQueue = mp.Queue()

        self.memoryInQueue = mp.Queue()

        self.parser = etree.XMLParser(recover=True, strip_cdata=True)
        
        self.qm = QueryManager(modeStat = False)

        self.dataProcess = mp.Process(target=processDataCollector, args=(self.entryQueue, self))
        self.queryProcess = mp.Process(target=processQueryCollector, args=(self.validationQueue, self))
        self.entryProcess = mp.Process(target=processBGPDiscover, args=(self.entryQueue, self.validationQueue, self))
        self.validationProcess = mp.Process(target=processValidation, args=(self.validationQueue, self.memoryInQueue, self))
        self.memoryProcess = mp.Process(target=processMemory, args=(self, gap*3, self.memoryInQueue))

        self.dataProcess.start()
        self.queryProcess.start()
        self.entryProcess.start()
        self.validationProcess.start()
        self.memoryProcess.start()

        self.mode = mode
        self.thread = None
        self.mesg = None
        if mode == 2 :
            self.socketserver = SocketServer(port=self.ports['DashboardEntry'], msgSize = 2048, ServerMsgProcessor = self )
        else:
            pass

    def run(self):
        self.socketserver.run()

    def processIn(self,msg) :
        req = json.loads(msg)

        path = req['path']
        print("[Dashboard]:",req)

        if path =='/run':
            rep = [self.nbBGP.value, self.nbREQ.value, self.stat['nbQueries'],  self.stat['sumPrecision'], self.stat['sumRecall'], self.stat['goldenNumber'], self.stat['genBGP'], self.stat['nbBGP'], self.nbQueries.value, self.nbEntries.value]

        elif path == '/save' :
            self.saveMemory()
            self.saveUsers()
            rep = True

        elif path == '/sweep' :
            (nbm, memory) = self.getMemory()
            l = list()
            for j in range(min(nbm,self.nlast)):
                (i,idQ,queryCode, t,ip,query,bgp,precision,recall) = memory[nbm-j-1]
                sbgp = []
                if i==0:
                    for (s,p,o) in [(tp.s,tp.p,tp.o) for tp in bgp.tp_set]: 
                        sbgp.append( toStr(s,p,o)+' .' )
                    l.append( (str(nbm-j), bgp.client, str(bgp.time), sbgp,'','', "No query assigned",None,None) )
                else:
                    if bgp is not None:
                        for (s,p,o) in [(tp.s,tp.p,tp.o)  for tp in bgp.tp_set]: 
                            sbgp.append( toStr(s,p,o)+' .' )
                    else:
                        sbgp.append(['No BGP assigned !'])
                    l.append( (str(nbm-j), ip, str(t), sbgp, idQ, queryCode, query.strip(), precision, recall) )

            rep = (self.stat['nbQueries'], self.stat['nbBGP'], self.gap.__str__(), self.nbQueries.value, self.nbEntries.value, str(self.nlast), l )

        elif path == '/bestof-1' :
            r = self.getRankingBGPs()
            r.sort(key=itemgetter(2), reverse=True)
            l = []
            for (chgDate, bgp, freq, query, _, precision, recall) in r[:self.nlast] :
                if query is None: query = ''
                sbgp = []
                for (s,p,o) in simplifyVars(bgp):
                    sbgp.append( toStr(s,p,o)+' .' )

                l.append( (sbgp, freq, query.strip())  )

            rep = (str(self.memDuration), l )

        elif path == '/bestof-2' :
            r = self.getTopKBGP(self.nlast)
            rep = []
            for e in r:
                (c, eVal) = e
                l = []
                for (s,p,o) in simplifyVars(eVal): 
                    l.append( toStr(s,p,o)+' .' )
                rep.append( (l, c.val) )

        elif path == '/bestof-3' :
            r = self.getRankingQueries()
            r.sort(key=itemgetter(2), reverse=True)
            l = []
            for (chgDate, bgp, freq, query, _, precision, recall) in r[:self.nlast] :
                if query is None: query = ''
                sbgp = []
                for (s,p,o) in simplifyVars(bgp):
                    sbgp.append( toStr(s,p,o)+' .' )

                l.append( (sbgp, freq, query.strip(), precision, recall)  )

            rep = (str(self.memDuration), l )

        elif path == '/bestof-4' :
            r = self.getTopKQueries(self.nlast)
            rep = []
            for e in r:
                (c, eVal) = e
                l = []
                for (s,p,o) in simplifyVars(eVal): 
                    l.append( toStr(s,p,o)+' .' )
                rep.append( (l, c.val) ) 

        elif path == '/bestof-5' :
            l = []
            with self.lck:
                for (ip,v) in sorted(self.usersMemory.items()) :
                    (nb, sumPrecision, sumRecall) = v
                    l.append( ( ip, nb, sumPrecision/nb, sumRecall/nb)  )
            rep = l

        else: rep = False

        return json.dumps(rep)


    def setTimeout(self, to):
        print('chg to:', to.total_seconds())
        self.timeout = to

    def swapOptimistic(self):
        self.optimistic = not(self.optimistic)

    def stop(self):
        # self.dataProcess.join()
        # self.queryProcess.join()
        self.entryProcess.join()
        self.validationProcess.join()
        self.memoryProcess.join()
        # self.saveMemory()
        # self.saveUsers()

    def getTopKBGP(self,n):
        with self.lck:
            res = self.topKBGPs[:n]
        return res

    def getTopKQueries(self,n):
        with self.lck:
            res = [e for e in self.topKQueries]
        return res[:n]

    def getRankingBGPs(self) :
        return self.cloneRanking(self.rankingBGPs)

    def getRankingQueries(self) :
        return self.cloneRanking(self.rankingQueries)

    def cloneRanking(self, ranking) :
        # print('Début clone Ranking')
        res = []
        with self.lck:
            for (t, d, n, query, ll, p, r) in ranking:
                res.append( (t, d, n, query, ll, p, r) )
        # print('Fin clone Ranking')
        return res

    def getMemory(self):
        # print('Début get memory')
        with self.lck:
            r = [(id, queryID,queryCode, time, ip, query, bgp, precision, recall) for (t, id, queryID,queryCode, time, ip, query, bgp, precision, recall) in self.memory]
        # print('Fin get memory')
        return (len(r), r )

    def saveMemory(self):
        file = 'sweep.csv'  # (id, time, ip, query, bgp, precision, recall)
        sep = '\t'
        exists = existFile(file)
        if exists: mode = "a"
        else: mode="w"
        maxt = self.lastTimeMemorySaved
        print('Saving memory ',mode)
        try:
            with open(file, mode, encoding='utf-8') as f:
                fn = ['id', 'qID', 'queryCode', 'time', 'ip', 'query', 'bgp', 'precision', 'recall']
                writer = csv.DictWriter(f, fieldnames=fn, delimiter=sep)
                if not(exists): writer.writeheader()
                with self.lck:
                    for (t, id, queryID,queryCode, t, ip, query, bgp, precision, recall) in self.memory:
                        if t > self.lastTimeMemorySaved:
                            maxt = max(t,self.lastTimeMemorySaved)
                            if bgp is not None:
                                bgp_txt = ".\n".join([tp.toStr() for tp in bgp.tp_set])
                            else:
                                bgp_txt = "..."
                            if query is None: query='...'
                            s = {'id': id, 'qID': queryID, 'queryCode':queryCode, 'time': t, 'ip': ip, 'query': query, 'bgp': bgp_txt, 'precision': precision, 'recall': recall}
                            writer.writerow(s)
            self.lastTimeMemorySaved = maxt
            print('Memory saved')
        except KeyboardInterrupt:
            print('Interupted') 

    def saveUsers(self):
        print('Saving users (%d) '%len(self.usersMemory.keys()))
        with open('sweep_users.csv',"w", encoding='utf-8') as f:
            fn=['ip','precision','recall','nb']
            writer = csv.DictWriter(f,fieldnames=fn,delimiter=',')
            writer.writeheader()
            with self.lck:
                for (ip,v) in self.usersMemory.items() :
                    (nb, sumPrecision, sumRecall) = v
                    s = dict({'ip':ip, 'precision':sumPrecision/nb,'recall':sumRecall/nb, 'nb':nb})
                    writer.writerow(s)


#==================================================
#==================================================
#==================================================
if __name__ == "__main__":
    print("main sweep")
    # gap = dt.timedelta(minutes=1)

    # tpq1 = TriplePatternQuery(Variable('s'), URIRef('http://exemple.org/p1'), Literal('2'), now(
    # ), 'Client2', [URIRef('http://exemple.org/test1'), URIRef('http://exemple.org/test2')], [], [])
    # tpq1.renameVars(1)
    # print(tpq1.toString())
    # print(tpq1.shape())
    # print(tpq1.iriJoinable())

    # tpq2 = TriplePatternQuery(URIRef('http://exemple.org/test1'), URIRef(
    #     'http://exemple.org/p2'), Literal('3'), now(), 'Client2', [], [], [])
    # tpq2.renameVars(2)
    # print(tpq2.toString())
    # print(tpq2.shape())
    # print(tpq2.iriJoinable())

    # tpq3 = TriplePatternQuery(URIRef('http://exemple.org/test2'), URIRef(
    #     'http://exemple.org/p2'), Literal('4'), now(), 'Client2', [], [], [])
    # tpq3.renameVars(3)
    # print(tpq3.toString())
    # print(tpq3.shape())
    # print(tpq3.iriJoinable())

    # print('---')

    # print('\n Début')
    # bgp = BasicGraphPattern(gap, tpq1)
    # bgp.print()

    # print('--- nestedLoopOf2')
    # print(tpq2.nestedLoopOf2(tpq1))

    # print('--- findNestedLoop')
    # (t, tp,fTp,mapVal) = bgp.findNestedLoop(tpq2)
    # if t:
    #     print(mapVal)
    #     print(fTp.toString())
    #     print(tp.toString())

    # print('---')


    # tpq4 = TriplePatternQuery(URIRef('http://exemple.org/test2'), URIRef(
    #     'http://exemple.org/p2'), Variable('o'), now(), 'Client2', [], [], ['a','b'])
    # tpq4.renameVars(4)
    # print(tpq4.toString())
    # print(tpq4.shape())
    # print(tpq4.iriJoinable())

    # bgp = BasicGraphPattern(gap, tpq4)
    # bgp.print()

    # tpq5 = TriplePatternQuery(URIRef('http://exemple.org/test1'), Variable('p'), Variable('o'), now(), 'Client2', [], ['p'], ['c','d'])
    # tpq5.renameVars(5)
    # print(tpq5.toString())
    # print(tpq5.shape())
    # print(tpq5.iriJoinable())

    # print(bgp.findIRIJoin(tpq5))

    # print('Fin')
    parser = argparse.ArgumentParser(description='SWEEP')
    parser.add_argument("-f", "--config", default='', dest="cfg", help="Config file")
    args = parser.parse_args()

    cfg = ConfigParser(interpolation=ExtendedInterpolation())
    r = cfg.read(args.cfg)

    if r == [] :
        print('Config file unkown')
        exit()
    print(cfg.sections())
    sweepCfg = cfg['SWEEP']
    agap = float(sweepCfg['Gap'])
    atimeout = float(sweepCfg['TimeOut'])
    aOptimistic = sweepCfg.getboolean('Optimistic')
    anlast = int(sweepCfg['BGP2View'])
    aports = { 'DataCollector' : int(sweepCfg['DataCollector']), 'QueryCollector' : int(sweepCfg['QueryCollector']), 'DashboardEntry' : int(sweepCfg['DashboardEntry']) }
    if atimeout == 0:
        to = agap
    else:
        to = atimeout
    try :
        print('Starting SWEEP')
        ctx = SWEEP(dt.timedelta(minutes= agap),dt.timedelta(minutes= to),aOptimistic, aports,anlast,mode=2)
        ctx.run()
    except KeyboardInterrupt:
        pass
    finally :
        print('Stopping SWEEP')
        ctx.stop()
        print('SWEEP Stopped')
