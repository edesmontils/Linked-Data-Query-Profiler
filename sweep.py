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
from tools.tools import now, fromISO

from rdflib import Variable, URIRef, Literal

from lxml import etree  # http://lxml.de/index.html#documentation
from lib.bgp import serialize2string, egal, calcPrecisionRecall, canonicalize_sparql_bgp, serializeBGP

from collections import OrderedDict

from functools import reduce

#==================================================

SWEEP_IN_ENTRY = 1
SWEEP_IN_DATA = 2
SWEEP_IN_END = 3

SWEEP_IN_QUERY = 4
SWEEP_OUT_QUERY = 5
SWEEP_IN_BGP = 6

SWEEP_START_SESSION = -1
SWEEP_END_SESSION = -2
SWEEP_PURGE = -3

SWEEP_ENTRY_TIMEOUT = 0.8  # percentage of the gap
SWEEP_PURGE_TIMEOUT = 0.1  # percentage of the gap

SWEEP_DEBUG_BGP_BUILD = True
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
        assert isinstance(
            tpq, TriplePatternQuery), "BasicGraphPattern.Add : Pb type TPQ"
        assert (self.client == 'Unknown') or (self.client ==
                                              tpq.client), "BasicGraphPattern.Add : client différent"
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

def processAgregator(in_queue, out_queue, val_queue, ctx):
    entry_timeout = ctx.gap*SWEEP_ENTRY_TIMEOUT
    purge_timeout = (ctx.gap*SWEEP_PURGE_TIMEOUT).total_seconds()
    currentTime = now()
    elist = dict()
    try:
        inq = in_queue.get()
        while inq is not None:
            (id, x, val) = inq
            if x == SWEEP_IN_ENTRY:
                (s, p, o, t, cl) = val
                currentTime = now()
                elist[id] = (s, p, o, currentTime, cl, set(), set(), set())
            elif x == SWEEP_IN_DATA:
                if id in elist:  # peut être absent car purgé
                    (s, p, o, t, _, sm, pm, om) = elist[id]
                    (xs, xp, xo) = val
                    currentTime = max(currentTime, t) + \
                        dt.timedelta(microseconds=1)
                    if isinstance(s, Variable):
                        sm.add(xs)
                    if isinstance(p, Variable):
                        pm.add(xp)
                    if isinstance(o, Variable):
                        om.add(xo)
            elif x == SWEEP_IN_END:
                mss = elist.pop(id, None)
                if mss is not None:  # peut être absent car purgé
                    out_queue.put((id, mss))
            elif x == SWEEP_START_SESSION:
                # print('Agregator - Start Session')
                currentTime = now()
                elist.clear()
                out_queue.put((id, SWEEP_START_SESSION))
            elif x == SWEEP_END_SESSION:
                # print('Agregator - End Session')
                currentTime = now()
                for v in elist:
                    out_queue.put((v, elist.pop(v)))
                out_queue.put((id, SWEEP_END_SESSION))
            else:  # SWEEP_PURGE...
                out_queue.put((id, SWEEP_PURGE))

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
                # print('purge')
                currentTime = now()
                inq = (0, SWEEP_PURGE, None)
    except KeyboardInterrupt:
        # penser à purger les dernières entrées -> comme une fin de session
        pass
    finally:
        for v in elist:
            out_queue.put((v, elist.pop(v)))
    out_queue.put(None)
    val_queue.put(None)

#==================================================


def processBGPDiscover(in_queue, out_queue, val_queue, ctx):
    gap = ctx.gap
    BGP_list = []
    try:
        entry = in_queue.get()
        while entry != None:
            (id, val) = entry
            if val == SWEEP_PURGE:
                val_queue.put((SWEEP_PURGE, 0, None))
            elif val == SWEEP_START_SESSION:
                # print('BGPDiscover - Start Session')
                BGP_list.clear()
                out_queue.put(SWEEP_START_SESSION)
            elif val == SWEEP_END_SESSION:
                # print('BGPDiscover - End Session')
                for bgp in BGP_list:
                    out_queue.put(bgp)
                    val_queue.put((SWEEP_IN_BGP, -1, bgp))
                BGP_list.clear()
                out_queue.put(SWEEP_END_SESSION)
            else:
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
                out_queue.put(bgp)
                val_queue.put((SWEEP_IN_BGP, -1, bgp))
            BGP_list = recent
            ctx.nbBGP.value = len(BGP_list)
            entry = in_queue.get()
    except KeyboardInterrupt:
        # penser à purger les derniers BGP ou uniquement autoutr du get pour gérer fin de session
        pass
    finally:
        for bgp in BGP_list:
            out_queue.put(bgp)
            val_queue.put((SWEEP_IN_BGP, -1, bgp))
        BGP_list.clear()
        ctx.nbBGP.value = 0
    out_queue.put(None)

#==================================================


def testPrecisionRecallBGP(queryList, bgp, gap):
    best = 0
    test = [(tp.s, tp.p, tp.o) for tp in bgp.tp_set]
    # print(test)
    best_precision = 0
    best_recall = 0
    for i in queryList:
        ((time, ip, query, qbgp, queryID),
         old_bgp, precision, recall) = queryList[i]

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
        ((time, ip, query, qbgp, queryID), old_bgp,
         precision, recall) = queryList[best]
        queryList[best] = ((time, ip, query, qbgp, queryID),
                           bgp, best_precision, best_recall)
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


def addBGP2Rank(bgp, nquery, line, precision, recall, ranking):
    ok = False
    for (i, (d, n, query, ll, p, r)) in enumerate(ranking):
        if bgp == d:
            ok = True
            break
    if ok:
        ll.add(line)
        if query == '':
            query = nquery
        ranking[i] = (d, n+1, query, ll, p+precision, r+recall)
    else:
        ranking.append((bgp, 1, nquery, {line}, precision, recall))


def processValidation(in_queue, ctx):
    valGap = ctx.gap * 2
    gap = ctx.gap
    currentTime = now()
    queryList = OrderedDict()
    try:
        inq = in_queue.get()
        while inq is not None:
            (mode, id, val) = inq

            if mode == SWEEP_IN_QUERY:
                with ctx.lck:
                    ctx.stat['nbQueries'] += 1
                (time, ip, query, qbgp, queryID) = val
                currentTime = now()
                if SWEEP_DEBUB_PR:
                    print('+++')
                    print(currentTime, ' New query', val)
                (precision, recall, bgp) = (0, 0, None)
                queryList[id] = (
                    (time, ip, query, qbgp, queryID), bgp, precision, recall)

            elif mode == SWEEP_IN_BGP:
                ctx.stat['nbBGP'] += 1
                bgp = val
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
                    ctx.memory.append(
                        (0, '', old_bgp.birthTime, old_bgp.client, None, old_bgp, 0, 0))
                    addBGP2Rank(canonicalize_sparql_bgp(
                        [(tp.s, tp.p, tp.o) for tp in old_bgp.tp_set]), '', id, 0, 0, ctx.rankingBGPs)

            # dans le cas où le client TPF n'a pas pu exécuter la requête...
            elif mode == SWEEP_OUT_QUERY:
                # suppress query 'queryID'
                for i in queryList:
                    ((time, ip, query, qbgp, queryID),
                     bgp, precision, recall) = queryList[i]
                    if queryID == val:
                        if SWEEP_DEBUB_PR:
                            print('---')
                            print(currentTime, ' Deleting query', queryID)
                        queryList.pop(i)
                        with ctx.lck:
                            ctx.stat['nbQueries'] -= 1
                        if bgp is not None:
                            if SWEEP_DEBUB_PR:
                                print('-')
                                print('extract its BGP')
                                bgp.print()
                            old_bgp = testPrecisionRecallBGP(
                                queryList, bgp, gap)
                            if old_bgp is not None:
                                ctx.memory.append(
                                    (0, '', old_bgp.birthTime, old_bgp.client, None, old_bgp, 0, 0))
                                addBGP2Rank(canonicalize_sparql_bgp(
                                    [(tp.s, tp.p, tp.o) for tp in old_bgp.tp_set]), '', id, 0, 0, ctx.rankingBGPs)
                        else:
                            if SWEEP_DEBUB_PR:
                                print('-')
                                print('No BGP to extract')
                        break

            else:  # mode == SWEEP_PURGE
                currentTime = now()

            # Suppress older queries
            old = []
            for id in queryList:
                ((time, ip, query, qbgp, queryID), bgp,
                 precision, recall) = queryList[id]
                if currentTime - time > valGap:
                    old.append(id)

            for id in old:
                ((time, ip, query, qbgp, queryID), bgp,
                 precision, recall) = queryList.pop(id)
                if SWEEP_DEBUB_PR:
                    print('--- purge ', queryID, '(', time, ') ---',
                          precision, '/', recall, '---', ' @ ', currentTime, '---')
                    print(query)
                    print('---')
                ctx.memory.append(
                    (id, queryID, time, ip, query, bgp, precision, recall))
                ctx.stat['sumRecall'] += recall
                ctx.stat['sumPrecision'] += precision
                ctx.stat['sumQuality'] += (recall+precision)/2
                if bgp is not None:
                    if SWEEP_DEBUB_PR:
                        print(".\n".join([toStr(s, p, o) for (
                            itp, (s, p, o), sm, pm, om) in bgp.tp_set]))
                    ctx.stat['sumSelectedBGP'] += 1
                    #---
                    assert ip == bgp.client, 'Client Query différent de client BGP'
                    #---
                    addBGP2Rank(canonicalize_sparql_bgp(qbgp), query,
                                id, precision, recall, ctx.rankingQueries)
                    addBGP2Rank(canonicalize_sparql_bgp(
                        [(tp.s, tp.p, tp.o) for tp in bgp.tp_set]), query, id, 0, 0, ctx.rankingBGPs)
                else:
                    if SWEEP_DEBUB_PR:
                        print('Query not assigned')
                    addBGP2Rank(qbgp, query, id, precision,
                                recall, ctx.rankingQueries)
                if SWEEP_DEBUB_PR:
                    print('--- --- @'+ip+' --- ---')
                    print(' ')
            ctx.nbREQ.value = len(queryList)
            inq = in_queue.get()
    except KeyboardInterrupt:
        # penser à afficher les dernières queries ou uniquement autour du get pour fin de session
        pass
    finally:
        for id in queryList:
            ((time, ip, query, qbgp, queryID), bgp,
             precision, recall) = queryList.pop(id)
            if SWEEP_DEBUB_PR:
                print('--- purge ', queryID, '(', time, ') ---',
                      precision, '/', recall, '---', ' @ ', currentTime, '---')
                print(query)
                print('---')
            ctx.memory.append(
                (id, queryID, time, ip, query, bgp, precision, recall))
            ctx.stat['sumRecall'] += recall
            ctx.stat['sumPrecision'] += precision
            ctx.stat['sumQuality'] += (recall+precision)/2
            if bgp is not None:
                if SWEEP_DEBUB_PR:
                    print(".\n".join([tp.toStr() for tp in bgp.tp_set]))
                ctx.stat['sumSelectedBGP'] += 1
                #---
                assert ip == bgp.client, 'Client Query différent de client BGP'
                #---
                addBGP2Rank(canonicalize_sparql_bgp(qbgp), query,
                            id, precision, recall, ctx.rankingQueries)
                addBGP2Rank(canonicalize_sparql_bgp(
                    [(tp.s, tp.p, tp.o) for tp in bgp.tp_set]), query, id, 0, 0, ctx.rankingBGPs)
            else:
                if SWEEP_DEBUB_PR:
                    print('Query not assigned')
                addBGP2Rank(qbgp, query, id, precision,
                            recall, ctx.rankingQueries)
            if SWEEP_DEBUB_PR:
                print('--- --- @'+ip+' --- ---')
                print(' ')
        ctx.nbREQ.value = 0

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


def processStat(ctx, duration):
    try:
        while True:
            time.sleep(duration.total_seconds())
            ctx.saveMemory()
    except KeyboardInterrupt:
        pass

#==================================================


class SWEEP:  # Abstract Class
    def __init__(self, gap, to, opt):
        #---
        assert isinstance(gap, dt.timedelta)
        #---
        self.gap = gap
        self.timeout = to
        self.optimistic = opt  # màj de la date du BGP avec le dernier TP reçu ?

        self.lck = mp.Lock()
        manager = mp.Manager()
        self.memory = manager.list()
        self.rankingBGPs = manager.list()
        self.rankingQueries = manager.list()
        # self.avgPrecision = mp.Value('f',0.0)
        # self.avgRecall = mp.Value('f',0.0)
        # self.avgQual = mp.Value('f',0.0)
        # self.Acuteness = mp.Value('f',0.0)

        self.nbBGP = mp.Value('i', 0)
        self.nbREQ = mp.Value('i', 0)

        self.qId = mp.Value('i', 0)
        self.stat = manager.dict({'sumRecall': 0, 'sumPrecision': 0,
                                  'sumQuality': 0, 'nbQueries': 0, 'nbBGP': 0, 'sumSelectedBGP': 0})

        self.dataQueue = mp.Queue()
        self.entryQueue = mp.Queue()
        self.validationQueue = mp.Queue()
        self.resQueue = mp.Queue()

        self.dataProcess = mp.Process(target=processAgregator, args=(
            self.dataQueue, self.entryQueue, self.validationQueue, self))
        self.entryProcess = mp.Process(target=processBGPDiscover, args=(
            self.entryQueue, self.resQueue, self.validationQueue, self))
        self.validationProcess = mp.Process(
            target=processValidation, args=(self.validationQueue, self))
        self.statProcess = mp.Process(target=processStat, args=(self, gap*3))

        self.dataProcess.start()
        self.entryProcess.start()
        self.validationProcess.start()
        self.statProcess.start()

    def setTimeout(self, to):
        print('chg to:', to.total_seconds())
        self.timeout = to

    def swapOptimistic(self):
        self.optimistic = not(self.optimistic)

    def startSession(self):
        self.dataQueue.put((0, SWEEP_START_SESSION, ()))

    def endSession(self):
        self.dataQueue.put((0, SWEEP_END_SESSION, ()))

    def put(self, v):
        self.dataQueue.put(v)
        # To implement

    def putQuery(self, time, ip, query, bgp, queryID):
        with self.qId.get_lock():
            self.qId.value += 1
            qId = self.qId.value
            if queryID is None:
                queryID = 'id'+str(qId)
        self.validationQueue.put(
            (SWEEP_IN_QUERY, qId, (time, ip, query, bgp, queryID)))

    def putEnd(self, i):
        self.dataQueue.put((i, SWEEP_IN_END, ()))

    def putEntry(self, i, s, p, o, time, client):
        self.dataQueue.put((i, SWEEP_IN_ENTRY, (s, p, o, time, client)))

    def putData(self, i, xs, xp, xo):
        self.dataQueue.put((i, SWEEP_IN_DATA, (xs, xp, xo)))

    def putLog(self, entry_id, entry):
        # (s,p,o,t,c,sm,pm,om) = entry
        self.entryQueue.put((entry_id, entry))

    def delQuery(self, x):
        self.validationQueue.put((SWEEP_OUT_QUERY, 0, x))

    def get(self):
        try:
            r = self.resQueue.get()
            if r == SWEEP_START_SESSION:
                return self.get()
            if r == SWEEP_END_SESSION:
                return None
            else:
                return r
        except KeyboardInterrupt:
            return None

    def stop(self):
        self.dataQueue.put(None)
        self.dataProcess.join()
        self.entryProcess.join()
        self.validationProcess.join()
        self.statProcess.join()
        # self.saveMemory()

    def saveMemory(self):
        file = 'sweep.csv'  # (id, time, ip, query, bgp, precision, recall)
        sep = '\t'
        with open(file, "w", encoding='utf-8') as f:
            fn = ['id', 'qID', 'time', 'ip', 'query',
                  'bgp', 'precision', 'recall']
            writer = csv.DictWriter(f, fieldnames=fn, delimiter=sep)
            writer.writeheader()
            for (id, queryID, t, ip, query, bgp, precision, recall) in self.memory:
                if bgp is not None:
                    bgp_txt = ".\n".join([tp.toStr() for tp in bgp.tp_set])
                else:
                    bgp_txt = "..."
                s = {'id': id, 'qID': queryID, 'time': t, 'ip': ip, 'query': query,
                     'bgp': bgp_txt, 'precision': precision, 'recall': recall}
                writer.writerow(s)


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
