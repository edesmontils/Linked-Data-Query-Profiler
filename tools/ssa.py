#!/usr/bin/env python3.6
# coding: utf8
"""
Tools to manage statistics on processes
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

# [MAA05]
# Ahmed Metwally, Divyakant Agrawal, and Amr Abbadi. 
# "Efficient computation of frequent and top-k elements in data streams." 
# Database Theory-ICDT 2005. 
# Springer Berlin Heidelberg, pages 398–412. 2005. 
# 10.1007/978-3-540-30570-5_27 
# http://link.springer.com/chapter/10.1007/978-3-540-30570-5_27 

from pprint import pprint
from collections import OrderedDict
import math
import multiprocessing as mp

#==================================================

class Counter:
    def __init__(self, id, bucket = None):
        self.val = 0
        self.id = id
        self.epsilon = 0
        self.bucket = bucket

    def inc(self): self.val += 1

    def attach(self, bucket): self.bucket = bucket

    def detach(self): self.bucket = None

    def print(self): print('\t',self.bucket.val ,'#',self.id,'# -',self.val,'-',self.epsilon)

#==================================================

class Bucket:
    def __init__(self, c):
        self.d = OrderedDict()
        self.val = c.val
        self.add(c)

    def contains(self, id): return self.d[id]

    def add(self, c):
        self.d[c.id] = c
        c.attach(self)

    def remove(self,c):
        self.d.pop(c.id)
        c.detach()

    def replace(self,c):
        (oid,oc) = self.d.popitem(True)
        c.epsilon = oc.val
        c.val = oc.val
        self.add(c)
        return oc

    def empty(self): return len(self.d)==0

    def print(self):
        print('Bucket:',self.val)
        for (k,v) in self.d.items():
            v.print()

#==================================================

class SpaceSavingCounter:
    def __init__(self,m,phi=0.33):
        self.size = m               # max size of the counter set
        self.N = 0                  # Current stream size
        self.monitored = dict()     # elements that are counted
        self.bucketList = list()

        #for continuous frequent
        self.phi = phi
        self.ptrPhi = 0


    def add(self, e, eVal = None):
        print('Adding ',e,' to ',self.monitored)
        self.N += 1

        if e in self.monitored :
            print('It exists !')
            (c, _) = self.monitored[e]
            self.incrementCounter(c)
        else:
            print('It does\'nt exist !')
            c = Counter(e)
            if (len(self.monitored) >= self.size) :
                # print('replacing')
                b = self.bucketList[0]
                oc = b.replace(c)
                self.monitored.pop(oc.id)
            else:
                # print('adding')
                b = Bucket(c)
                self.bucketList.insert(0,b)
            self.incrementCounter(c)
            self.monitored[e]= (c, eVal)

        self.continuousQueryFrequent(c)
        # self.continuousQueryTopK(c)

        self.print()

    def print(self):
        for (i,b) in enumerate(self.bucketList):
            b.print()
            if i == self.ptrPhi: print('\t Frequent threshold')


    def incrementCounter(self, c) :
        b = c.bucket
        print(self.bucketList)
        ib = self.bucketList.index(b)
        if (ib < len(self.bucketList)-1 ) :
            bp = self.bucketList[ib+1]
        else: bp = None
        b.remove(c)
        c.inc()

        if (bp is not None) and (bp.val == c.val): bp.add(c)
        else:
            bucket = Bucket(c)
            self.bucketList.insert(ib+1,bucket)

        if b.empty(): 
            if ib <= self.ptrPhi: self.ptrPhi -= 1
            self.bucketList.pop(ib)


    def counterList(self,first=0):
        l = list()
        for (i,b) in enumerate(self.bucketList[first:]):
            l = l + list(b.d.values())
        return l

    def queryFrequent(self, phi = None):
        if phi == None: phi = self.phi
        guaranteed = True
        frqts = list()
        cl = self.counterList()
        cl.reverse()
        threshold = math.ceil(phi*self.N)
        print('Do frequents with ', threshold, ' on ',cl)
        for c in cl:
            if c.val <= threshold: break
            frqts.append(c.id)
            if (c.val-c.epsilon) < threshold:
                guaranteed = False
        return (guaranteed,frqts)

    def continuousQueryFrequent(self,c):
        bucketPhi = self.bucketList[self.ptrPhi]
        l = len(self.bucketList)
        threshold = math.ceil(self.phi*self.N)
        if (bucketPhi.val < threshold) and (self.ptrPhi < l-1) : self.ptrPhi += 1
        bucketC = c.bucket
        bucketPhi = self.bucketList[self.ptrPhi]
        if (bucketC.val > threshold) and (bucketC.val <= bucketPhi.val): 
            # print(c.id,' is considered as frequent')
            self.ptrPhi = self.bucketList.index(bucketC)

    def queryTopK(self,k):
        order = True
        guaranteed = False
        minGuarFreq = -1
        cl = self.counterList()
        cl.reverse()
        k = min(k, len(cl) )
        tk = list()
        for (i,c) in enumerate(cl[:k]):
            tk.append(c.id)
            if (minGuarFreq<0) or ( (c.val-c.epsilon) < minGuarFreq): minGuarFreq = c.val-c.epsilon
            if i<k-1 : 
                if (c.val-c.epsilon) < cl[i+1].val : order=False
        print(tk)
        return (guaranteed, order, tk)

    def continuousQueryTopK(self,c):
        pass


#==================================================
#==================================================
#==================================================

if __name__ == '__main__':
    print('main')
    ssa = SpaceSavingCounter(3)
    for car in ['x','y','t','y','x','s','y','z','y','x','s','z','y','x','x']: ssa.add(car)

    print(ssa.monitored)
    for c in ssa.counterList():
        c.print()
    print(ssa.queryFrequent(0.33))
    print(ssa.queryTopK(2))