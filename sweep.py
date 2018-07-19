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

import signal

import csv
from tools.tools import now, fromISO, existFile

from tools.ssa import *


from rdflib import Variable, URIRef, Literal

from lxml import etree  # http://lxml.de/index.html#documentation
from lib.bgp import serialize2string, egal, calcPrecisionRecall, canonicalize_sparql_bgp, serializeBGP

from collections import OrderedDict

from functools import reduce

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
        self.su = set()
        self.pu = set()
        self.ou = set()

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
                print('\t\tbsm:', listToStr(tpq.sm), '\n\t\t\tbsu:', listToStr(tpq.su),
                      '\n\t\tbpm:', listToStr(tpq.pm), '\n\t\t\tbpu:', listToStr(tpq.pu),
                      '\n\t\tbom:', listToStr(tpq.om), '\n\t\t\tbou:', listToStr(tpq.ou),)

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

def processAgregator(in_queue, out_queue, ctx):
    entry_timeout = ctx.gap*SWEEP_ENTRY_TIMEOUT
    purge_timeout = (ctx.gap*SWEEP_PURGE_TIMEOUT).total_seconds()
    currentTime = now()
    elist = dict()
    print('[processAgregator] Started :\n\t- entry_timeout :',entry_timeout,'\n\t- purge_timeout : ',purge_timeout)
    try:

        inq = in_queue.get()

        while inq is not None:
            (id, x, val) = inq
            if x == SWEEP_IN_ENTRY:
                # print('[processAgregator] New Entry')
                (s, p, o, t, cl) = val
                currentTime = now()
                elist[id] = (s, p, o, currentTime, cl, set(), set(), set())
            elif x == SWEEP_IN_DATA:
                # print('[processAgregator] New Data In')
                if id in elist:  # peut être absent car purgé
                    (s, p, o, t, _, sm, pm, om) = elist[id]
                    (xs, xp, xo) = val
                    currentTime = max(currentTime, t) + dt.timedelta(microseconds=1)
                    if isinstance(s, Variable): sm.add(xs)
                    if isinstance(p, Variable): pm.add(xp)
                    if isinstance(o, Variable): om.add(xo)
            elif x == SWEEP_IN_END:
                # print('[processAgregator] In ended')
                mss = elist.pop(id, None)
                if mss is not None:  # peut être absent car purgé
                    out_queue.put((id, mss))
            elif x == SWEEP_IN_LOG:
                # print('[processAgregator] New Log Entry')
                out_queue.put((id, val))
            else:  # SWEEP_PURGE...
                # print('[processAgregator] purge (%d waiting entries)'%len(elist))
                pass

            # purge les entrées trop vieilles !
            old = []
            for id in elist:
                (s, p, o, t, _, sm, pm, om) = elist[id]
                if (currentTime - t) > entry_timeout:
                    old.append(id)
            for id in old:
                v = elist.pop(id)
                out_queue.put((id, v))

            try:
                inq = in_queue.get(timeout=purge_timeout)
            except Empty:
                currentTime = now()
                inq = (0, SWEEP_PURGE, None)

    except KeyboardInterrupt:
        # penser à purger les dernières entrées -> comme une fin de session
        pass
    finally:
        for v in elist:
            out_queue.put((v, elist.pop(v)))
    out_queue.put(None)
    print('[processAgregator] Stopped')

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
                            print(
                                '-----------------------------------\n\t Etude avec BGP ', i)
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
                                        print('\t\t Ajout de ', new_tpq.toStr(
                                        ), '\n\t\t avec ', candTP.toStr())
                                    bgp.add(candTP, new_tpq.sign())
                                (vs,vp,vo) = mapVal
                                if vs is not None: fromTP.su.add(vs)
                                if vp is not None: fromTP.pu.add(vp)
                                if vo is not None: fromTP.ou.add(vo)
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
                ctx.stat['nbQueries'] += 1
                (time, ip, query, qbgp, queryID,queryCode) = val
                currentTime = now()
                if SWEEP_DEBUB_PR:
                    print('+++')
                    print(currentTime, ' New query', val)
                (precision, recall, bgp) = (0, 0, None)
                queryList[id] = ((time, ip, query, qbgp, queryID,queryCode), bgp, precision, recall)
                # print('[processValidation] Query added')

            elif mode == SWEEP_IN_BGP:
                bgp = val
                # print('[processValidation] BGP Analysis')
                if bgp is not None: # HEURISTIQUE : Un BGP qui contient un simple triplet n'est pas valide
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
                        ctx.stat['nbQueries'] -= 1
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

# todo :
# - faire deux mémoires pour les BGPs, une court terme sur 10*gap par exemple avec la méthode sûre et une long terme avec algo ssc.
# - faire une sauvegarde incrémentale pour permettre de ne concervé que ce qui est nécessaire pour la mémoire à court terme.
# - faire aussi les deux mémoires pour les requêtes

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

            if ((mode==0) and (nbMemoryChanges > 0)) or (nbMemoryChanges > 10): # Save memory in a CSV file
                # print('[processMemory] Save (%d entries to save ; %d rankedBGPs ; %d rankedQueries ; %d in memory)'%(nbMemoryChanges,len(ctx.rankingBGPs),len(ctx.rankingQueries),len(ctx.memory) ) )
                with ctx.lck:
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
                    if ip in ctx.usersMemory.keys():
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
        print('[processMemory] Stopped :\n\t- max memory size : ', maxNbMemory, '\n\t- max BGP ranking size : ',maxRankingBGPs ,'\n\t- max Queries ranking size : ',maxRankingQueries )

#==================================================

class SWEEP:  # Abstract Class
    def __init__(self, gap, to, opt, mem = 100):
        #---
        assert isinstance(gap, dt.timedelta)
        #---
        self.gap = gap
        self.timeout = to
        self.optimistic = opt  # màj de la date du BGP avec le dernier TP reçu ?

        self.lck = mp.Lock()
        self.manager = mp.Manager()
 
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

        self.nbBGP = mp.Value('i', 0)
        self.nbREQ = mp.Value('i', 0)

        self.qId = mp.Value('i', 0)
        self.stat = self.manager.dict({'sumRecall': 0, 'sumPrecision': 0,
                                  'sumQuality': 0, 'nbQueries': 0, 'nbBGP': 0, 'sumSelectedBGP': 0})

        self.dataQueue = mp.Queue()
        self.entryQueue = mp.Queue()
        self.validationQueue = mp.Queue()

        self.memoryInQueue = mp.Queue()

        self.dataProcess = mp.Process(target=processAgregator, args=(
            self.dataQueue, self.entryQueue, self))
        self.entryProcess = mp.Process(target=processBGPDiscover, args=(
            self.entryQueue, self.validationQueue, self))
        self.validationProcess = mp.Process(
            target=processValidation, args=(self.validationQueue, self.memoryInQueue, self))
        self.memoryProcess = mp.Process(target=processMemory, args=(self, gap*3, self.memoryInQueue))

        self.dataProcess.start()
        self.entryProcess.start()
        self.validationProcess.start()
        self.memoryProcess.start()

    def setTimeout(self, to):
        print('chg to:', to.total_seconds())
        self.timeout = to

    def swapOptimistic(self):
        self.optimistic = not(self.optimistic)

    def put(self, v):
        self.dataQueue.put(v)
        # To implement

    def putQuery(self, time, ip, query, bgp, queryID,queryCode):
        with self.qId.get_lock():
            self.qId.value += 1
            qId = self.qId.value
            if queryID is None:
                queryID = 'id'+str(qId)
        self.validationQueue.put(
            (SWEEP_IN_QUERY, qId, (time, ip, query, bgp, queryID,queryCode)))

    def putEnd(self, i):
        self.dataQueue.put((i, SWEEP_IN_END, ()))

    def putEntry(self, i, s, p, o, time, client):
        self.dataQueue.put((i, SWEEP_IN_ENTRY, (s, p, o, time, client)))

    def putData(self, i, xs, xp, xo):
        self.dataQueue.put((i, SWEEP_IN_DATA, (xs, xp, xo)))

    def putLog(self, entry_id, entry):
        # (s,p,o,t,c,sm,pm,om) = entry
        self.dataQueue.put((entry_id, SWEEP_IN_LOG, entry))

    def delQuery(self, x):
        self.validationQueue.put((SWEEP_OUT_QUERY, 0, x))

    def stop(self):
        self.dataQueue.put(None)
        self.dataProcess.join()
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
            for (ip,v) in self.usersMemory.items() :
                (nb, sumPrecision, sumRecall) = v
                s = dict({'ip':ip, 'precision':sumPrecision/nb,'recall':sumRecall/nb, 'nb':nb})
                writer.writerow(s)

class GracefulInterruptHandler(object):

    def __init__(self, sig=signal.SIGINT):
        self.sig = sig

    def __enter__(self):

        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):

        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True


#==================================================


def makeLog(ip):
    #print('Finding bgp')
    node_log = etree.Element('log')
    node_log.set('ip', ip)
    return node_log


def addBGP(n, bgp, node_log):
    #print(serializeBGP2str([ x for (x,sm,pm,om,h) in bgp.tp_set]))
    entry_node = etree.SubElement(node_log, 'entry')
    entry_node.set('datetime', '%s' % bgp.time)
    entry_node.set('logline', '%s' % n)
    request_node = etree.SubElement(entry_node, 'request')
    try:
        bgp_node = serializeBGP([(tp.s, tp.p, tp.o) for tp in bgp.tp_set])
        entry_node.insert(1, bgp_node)
        query = 'select * where{ \n'
        for tp in bgp.tp_set:
            query += serialize2string(tp.s) + ' ' + serialize2string(tp.p) + \
                ' ' + serialize2string(tp.o) + ' .\n'
        query += ' }'
        request_node.text = query
    except Exception as e:
        print('PB serialize BGP : %s\n%s\n%s', e.__str__(), query, bgp)
    return node_log


def save(node_log, lift2):
    try:
        print('Ecriture de "%s"' % lift2)
        tosave = etree.tostring(
            node_log,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            doctype='<!DOCTYPE log SYSTEM "http://documents.ls2n.fr/be4dbp/log.dtd">')
        try:
            f = open(lift2, 'w')
            f.write(tosave.decode('utf-8'))
        except Exception as e:
            print(
                'PB Test Analysis saving %s : %s',
                lift2,
                e.__str__())
        finally:
            f.close()
    except etree.DocumentInvalid as e:
        print('PB Test Analysis, %s not validated : %s' % (lift2, e))

#==================================================
#==================================================
#==================================================
if __name__ == "__main__":
    print("main sweep")
    gap = dt.timedelta(minutes=1)
    tpq1 = TriplePatternQuery(Variable('s'), URIRef('http://exemple.org/p1'), Literal('2'), now(
    ), 'Client2', [URIRef('http://exemple.org/test1'), URIRef('http://exemple.org/test2')], [], [])
    tpq1.renameVars(1)
    print(tpq1.toString())
    tpq2 = TriplePatternQuery(URIRef('http://exemple.org/test1'), URIRef(
        'http://exemple.org/p2'), Literal('3'), now(), '2', ['a', 'b'], [], [])
    tpq2.renameVars(2)
    print(tpq2.toString())
    tpq3 = TriplePatternQuery(URIRef('http://exemple.org/test2'), URIRef(
        'http://exemple.org/p2'), Literal('4'), now(), '2', ['a', 'b'], [], [])
    tpq3.renameVars(3)
    print(tpq3.toString())

    print('\n Début')
    bgp = BasicGraphPattern(gap, tpq1)
    bgp.print()

    print(tpq2.nestedLoopOf2(tpq1))

    (t, tp,fTp,mapVal) = bgp.findNestedLoop(tpq2)
    if t:
        print(tp.toString())
    print('Fin')
