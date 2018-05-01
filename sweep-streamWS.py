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

import multiprocessing as mp

import datetime as dt

import argparse
import html

from tools.tools import now, fromISO
from operator import itemgetter

from lxml import etree  # http://lxml.de/index.html#documentation
from lib.bgp import simplifyVars, unSerialize, unSerializeBGP
from lib.QueryManager import QueryManager

from io import StringIO
from tools.ssa import *

from sweep import SWEEP, toStr

from flask import Flask, render_template, request, jsonify
# http://flask.pocoo.org/docs/0.12/

from urllib.parse import urlparse, unquote_plus
from configparser import ConfigParser, ExtendedInterpolation

from rdflib import Variable

import requests as http


class Context(object):
    """docstring for Context"""
    def __init__(self):
        super(Context, self).__init__()
        self.sweep = None
        self.parser = etree.XMLParser(recover=True, strip_cdata=True)
        self.cpt = 0
        self.list = mp.Manager().list()
        self.to = 0.0
        self.gap = 0.0
        self.opt = False
        self.nlast = 10
        self.nbQueries = 0
        self.nbEntries = 0
        self.nbCancelledQueries = 0
        self.nbQBF = 0
        self.nbTO = 0
        self.nbEQ = 0
        self.nbOther = 0
        self.nbClientError = 0
        self.nbEmpty = 0
        self.qm = QueryManager(modeStat = False)
        self.entry_id = 0

ctx = Context()

#==================================================

# Initialize the Flask application
app = Flask(__name__)
# set the secret key.  keep this really secret:
app.secret_key = '\x0ctD\xe3g\xe1XNJ\x86\x02\x03`O\x98\x84\xfd,e/5\x8b\xd1\x11'

@app.route('/')
# @login_required
def index():
    return render_template('index-sweep.html',nom_appli="SWEEP Dashboard", version="0.2")

@app.route('/bestof')
def bo():
    t = '<table cellspacing="50"><tr>'

    rep = '<td><h1>Frequent deduced BGPs</h1> <p>(short term memory : %s ; %s more frequents)</p>'%(ctx.sweep.memDuration,str(ctx.nlast))
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td>'
    r = ctx.sweep.getRankingBGPs()
    r.sort(key=itemgetter(2), reverse=True)
    for (chgDate, bgp, freq, query, _, precision, recall) in r[:ctx.nlast]:
        if query is None: query = ''
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(bgp):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td><td>%s</td>'%(freq,html.escape(query))
        rep += '</tr>'
    rep += '</table></td>'

    tk = ctx.sweep.getTopKBGP(ctx.nlast)
    rep += '<td><h1>Frequent deduced BGPs [MAA05]</h1><p>(long term memory ; '+str(ctx.nlast)+' more frequents)</p>'
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td>'
    for e in tk:
        (c, eVal) = e
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(eVal):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td>'%c.val
        rep += '</tr>'
    rep += '</table></td>'

    rep += '<td><h1>Frequent Ground Truth Queries</h1> <p>(short term memory : %s ; %s more frequents)</p>'%(ctx.sweep.memDuration,str(ctx.nlast))
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td><td>Avg. Precision</td><td>Avg. Recall</td>'
    r = ctx.sweep.getRankingQueries()
    r.sort(key=itemgetter(2), reverse=True)
    for (chgDate, bgp, freq, query, _, precision, recall) in r[:ctx.nlast]:
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(bgp):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td><td>%s</td><td>%2.3f</td><td>%2.3f</td>'%(freq,html.escape(query), precision/freq, recall/freq)
        rep += '</tr>'
    rep += '</table></td>'


    tk = ctx.sweep.getTopKQueries(ctx.nlast)
    rep += '<td><h1>Frequent Ground Truth Queries [MAA05]</h1> <p>(long term memory ; %s more frequents)</p>'%str(ctx.nlast)
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td>'
    for e in tk:
        (c, eVal) = e
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(eVal):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td>'%c.val
        rep += '</tr>'
    rep += '</table></td>'

    t += rep + '<tr></table>'

    return rep

@app.route('/pr')
def doPR():
    nb = ctx.sweep.stat['nbQueries']
    #nbbgp = ctx.sweep.stat['nbBGP']
    if nb>0:
        avgPrecision = ctx.sweep.stat['sumPrecision']/nb
        avgRecall = ctx.sweep.stat['sumRecall']/nb
        #avgQual = ctx.sweep.stat['sumQuality']/nb
    else:
        avgPrecision = 0
        avgRecall = 0
        #avgQual = 0
    #if nbbgp>0 :
    #    Acuteness = ctx.sweep.stat['sumSelectedBGP'] / nbbgp
    #else:
    #    Acuteness = 0

    return jsonify(result=(avgPrecision,avgRecall))

@app.route('/sweep')
def sweep():
    ctx.cpt += 1
    nb = ctx.sweep.stat['nbQueries']
    nbbgp = ctx.sweep.stat['nbBGP']
    #if nb>0:
        #avgPrecision = ctx.sweep.stat['sumPrecision']/nb
        #avgRecall = ctx.sweep.stat['sumRecall']/nb
        #avgQual = ctx.sweep.stat['sumQuality']/nb
    #else:
        #avgPrecision = 0
        #avgRecall = 0
        #avgQual = 0
    #if nbbgp>0 :
    #    Acuteness = ctx.sweep.stat['sumSelectedBGP'] / nbbgp
    #else:
    #    Acuteness = 0

    rep = '<h1>Information</h1><table><tr><td>'

    rep += '<table  cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Gap (hh:mm:ss)</td>'
    rep += '</thead><tr>'
    rep += '<td>%s</td>'%(dt.timedelta(minutes= ctx.gap))
    rep += '</tr></table>'

    rep += '</td><td>'

    rep += '<table cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Evaluated Queries</td>'

    rep += '<td>BGP</td><td>TPQ</td>'
    rep += '</thead><tr>'
    rep += '<td>%d / %d</td>'%(nb,ctx.nbQueries)

    rep += '<td>%d</td><td>%d</td>'%(nbbgp,ctx.nbEntries)
    rep += '</tr></table>'
    rep += '</td><td>'

    rep += '</td></tr></table>\n'

    rep += '<h1>Deduced BGPs</h1><p>('+str(ctx.nlast)+' more recents)</p><table cellspacing="1" border="1" cellpadding="5"  width="100%">\n'
    rep += '<thead><td></td><td>ip</td><td>time</td><td width="35%">bgp</td><td  width="45%">Original query</td><td>Precision</td><td>Recall</td>'
    rep += '</thead>\n'
    (nb, memory) = ctx.sweep.getMemory()
    for j in range(min(nb,ctx.nlast)):
        (i,idQ, t,ip,query,bgp,precision,recall) = memory[nb-j-1]
        if i==0:
            rep +='<tr><td>'+str(nb-j)+'</td><td>'+bgp.client+'</td><td>'+str(bgp.time)+'</td><td>'
            for (s,p,o) in [(tp.s,tp.p,tp.o) for tp in bgp.tp_set]:
                rep += html.escape(toStr(s,p,o))+' . <br/>'
            rep += '</td><td>No query assigned</td><td></td><td></td>'
            rep += '</tr>'
        else:
            rep +='<tr><td>'+str(nb-j)+'</td><td>'+ip+'</td><td>'+str(t)+'</td><td>'
            if bgp is not None:
                for (s,p,o) in [(tp.s,tp.p,tp.o)  for tp in bgp.tp_set]:
                    rep += html.escape(toStr(s,p,o))+' . <br/>'
            else:
                rep += 'No BGP assigned !'
            rep += '</td><td>'+idQ+'<br/>'+html.escape(query)+'</td><td>%2.3f</td><td>%2.3f</td>'%(precision,recall)    # .replace('\n','<br/>')
            rep += '</tr>'
    rep += '</table>'
    return rep

@app.route('/run', methods=['get'])
def doRun():
    return jsonify(result=(ctx.sweep.nbBGP.value, ctx.sweep.nbREQ.value))

@app.route('/inform', methods=['post','get'])
def processInform():
    if request.method == 'POST':

        #ip = request.remote_addr

        errtype = request.form['errtype']
        queryNb = request.form['no']
        if errtype == 'QBF':
            print('(%s)'%queryNb,'Query Bad Formed :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbQBF += 1
        elif errtype == 'TO':
            print('(%s)'%queryNb,'Time Out :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbTO += 1
        elif errtype == 'CltErr':
            print('(%s)'%queryNb,'TPF Client Error for :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbClientError += 1
        elif errtype == 'EQ':
            print('(%s)'%queryNb,'Error Query for :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbEQ += 1
        elif errtype == 'Other':
            print('(%s)'%queryNb,'Unknown Pb for query :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbOther += 1
        elif errtype == 'Empty':
            print('(%s)'%queryNb,'Empty for :',request.form['data'])
            ctx.nbEmpty += 1
        else:
            print('(%s)'%queryNb,'Unknown Pb for query :',request.form['data'])
            ctx.sweep.delQuery(queryNb)
            ctx.nbCancelledQueries += 1
            ctx.nbOther += 1
        return jsonify(result=True)
    else:
        print('"inform" not implemented for HTTP GET')
        return jsonify(result=False)

@app.route('/query', methods=['post','get'])
def processQuery():
    if request.method == 'POST':
        print(request.headers)
        data = request.form['data']

        # print('Receiving request:',data)
        try:
            tree = etree.parse(StringIO(data), ctx.parser)
            q = tree.getroot()
            ip_remote = request.remote_addr
            client = q.get('client')
            if client is None :
                if 'x-forwarded-for' in request.headers:
                    client = request.headers['x-forwarded-for']
                elif 'X-Forwarded-For' in request.headers:
                    client = request.headers['X-Forwarded-For']

                if client is None:
                    q.set('client',str(ip_remote) )
                elif client in ["undefined","", "undefine"]:
                    q.set('client',str(ip_remote) )
                elif "::ffff:" in client:
                    q.set('client', client[7:])
                else : q.set('client',client)

            print('QUERY - ip-remote:',ip_remote,' client:',client, ' choix:',q.get('client'))
            ip = q.get('client')

            query = q.text
            time = fromISO(q.attrib['time']) # now()# 
            print('@ ', time)
            # print('fromISO ', fromISO(q.attrib['time']) )
            print('now', now())

            if query.startswith('#bgp-list#') :
                t = query.split('\n')
                bgp_list = unquote_plus(t[0][10:])
                del t[0]
                query = '\n'.join(t)
            else:
                bgp_list = '<l/>'

            l = []
            print(query)
            print(bgp_list)
            lbgp = etree.parse(StringIO(bgp_list), ctx.parser)
            for x in lbgp.getroot():
                bgp = unSerializeBGP(x)
                l.append(bgp)

            if len(l) == 0:
                print('BGP list empty... extracting BGP from the query')
                (bgp,nquery) = ctx.qm.extractBGP(query)
                query = nquery
                l.append(bgp)

            queryID = request.form['no']

            ctx.nbQueries += len(l)
            if queryID =='ldf-client':
                pass #queryID = queryID + str(ctx.nbQueries)
            print('ID',queryID)
            rang = 0
            for bgp in l :
                rang += 1
                ctx.sweep.putQuery(time,ip,query,bgp,str(queryID)+'_'+str(rang))

            return jsonify(result=True)
        except Exception as e:
            print('Exception',e)
            print('About:',data)
            return jsonify(result=False)
    else:
        print('"query" not implemented for HTTP GET')
        return jsonify(result=False)

# data 2017-10-19T09:18:03.231Z

@app.route('/data', methods=['post','get'])
def processData():
    if request.method == 'POST':
        data = request.form['data']
        i = request.form['no']
        time = fromISO(request.form['time'])

        #print(i,time)
        # #print('Receiving data:',data)

        ip = request.remote_addr
        ip2 = request.form['ip']

        client = ip2 # request.form['ip']
        if client is None:
            client = ip
        elif client in ["undefined","", "undefine"]:
            client = ip
        elif "::ffff:" in client:
            client = client[7:]

        #print('DATA - ip-remote:',ip,' ip-post:',ip2, ' choix:',client)

        try:
            tree = etree.parse(StringIO(data), ctx.parser)
            ctx.nbEntries += 1
            entry = (None, None, None, now(), None, set(),set(),set())
            #currentTime = now()
            entry_id = ctx.entry_id
            ctx.entry_id += 1
            for e in tree.getroot():
                if e.tag == 'e':
                    if e[0].get('type')=='var' : e[0].set('val','s')
                    if e[1].get('type')=='var' : e[1].set('val','p')
                    if e[2].get('type')=='var' : e[2].set('val','o')
                    s = unSerialize(e[0])
                    p = unSerialize(e[1])
                    o = unSerialize(e[2])

                    #currentTime = now()
                    entry = (s,p,o,time,client,set(),set(),set())

                    # print('new TPQ : ',toStr(s,p,o))

                elif e.tag == 'd':
                    xs = unSerialize(e[0])
                    xp = unSerialize(e[1])
                    xo = unSerialize(e[2])

                    (s,p,o,t,c,sm,pm,om) = entry
                    # currentTime = max(currentTime,t) + dt.timedelta(microseconds=1)
                    if isinstance(s,Variable): sm.add(xs)
                    if isinstance(p,Variable): pm.add(xp)
                    if isinstance(o,Variable): om.add(xo)

                    entry = (s,p,o,t,c,sm,pm,om)

                    # ctx.sweep.putData(i, xs, xp, xo)
                    # print('new data : ',toStr(xs,xp,xo))

                elif e.tag == 'm':
                    # s = unSerialize(e[0])
                    # p = unSerialize(e[1])
                    # o = unSerialize(e[2])
                    # print('new meta : ',toStr(s,p,o))
                    pass
                else:
                    pass

            # ctx.sweep.putEnd(i)
            ctx.sweep.putLog(entry_id,entry)

            return jsonify(result=True)
        except Exception as e:
            print('Exception',e)
            print('About:',data)
            return jsonify(result=False)
    else:
        print('"data" not implemented for HTTP GET')
        return jsonify(result=False)

@app.route('/mentions')
def mentions():
    s = """
        <p>This small web application has been developed for demonstration purposes. It can not therefore be used for any other purpose. It shall be made available in so far as its use is not diverted. The author can not be held responsible for malfunctions or loss of data in case of misuse and reserves the right to delete it at any time.</p>
        <p>Application developped et tested with Python 3.5 and 3.6.</p>
        <p>Design adapted from "<a href="http://www.freecsstemplates.org/preview/dusplic/">dusplic</a>" de <a href="http://www.freecsstemplates.org/"><strong>Free CSS Templates</strong></a>, under license <a href="./license.txt">Creative Common</a>.</p>
        <p>Icons from <a href="http://www.iconspedia.com/">http://www.iconspedia.com/</a> in the set "<a href="http://www.iconspedia.com/pack/basic-set-2061/">Basic set</a>" of PixelMixer (<a href="http://pixel-mixer.com/">http://pixel-mixer.com/</a>) under license CC-by-sa.<br/>
        <!--img src="http://www.iconspedia.com/common/images/logo.jpg" width="100" alt="CC-by-sa"/--></p>
        <p>Effects and JavaScript frameworks <a href="http://www.prototypejs.org">prototypejs.org<!--img src="http://www.prototypejs.org/images/logo-home.gif" alt="prototypejs.org" /--></a> et <a href="http://www.script.aculo.us">script.aculo.us<!--img src="http://www.script.aculo.us/scriptaculous_logo.png" width="300" alt="script.aculo.us"/--></a>.</p>
        <p>SWEEP uses <a href="http://justgage.com/">JustGage</a> (under <a href="http://opensource.org/licenses/mit-license.php">MIT License</a>), a JavaScript plugin for generating and animating gauges.</p>
        <p>(c) E. Desmontils &amp; P. Serrano-Alvarado, University of Nantes, France, 2017</p>
    """
    return s

#==================================================
#==================================================
#==================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linked Data Query Profiler (for a modified TPF server)')
    # parser.add_argument('files', metavar='file', nargs='+',help='files to analyse')
    parser.add_argument("-g", "--gap", type=float, default=60, dest="gap", help="Gap in minutes (60 by default)")
    parser.add_argument("-to", "--timeout", type=float, default=0, dest="timeout",
                        help="TPF server Time Out in minutes (%d by default). If '-to 0', the timeout is the gap." % 0)
    parser.add_argument("-o","--optimistic", help="BGP time is the last TP added (False by default)",
                    action="store_true",dest="doOptimistic")
    parser.add_argument("-l", "--last", type=int, default=10, dest="nlast", help="Number of last BGPs to view (10 by default)")
    parser.add_argument("--host", default="0.0.0.0", dest="host", help="host ('0.0.0.0' by default)")
    parser.add_argument("--port", type=int, default=5000, dest="port", help="Port (5000 by default)")
    # parser.add_argument("--chglientMode", dest="chglientMode", action="store_true", help="Do TPF Client mode")

    parser.add_argument("-f", "--config", default='', dest="cfg", help="Config file")

    args = parser.parse_args()

    if (args.cfg == '') :
        ahost = args.host
        aport = args.port
        agap = args.gap        
        atimeout = args.timeout
        aurl = 'http://'+ahost+":"+str(aport)
        aOptimistic = args.doOptimistic
        anlast = args.nlast
    else :
        cfg = ConfigParser(interpolation=ExtendedInterpolation())
        r = cfg.read(args.cfg)
        if r == [] :
            print('Config file unkown')
            exit()
        print(cfg.sections())
        sweepCfg = cfg['SWEEP-WS']
        agap = float(sweepCfg['Gap'])
        atimeout = float(sweepCfg['TimeOut'])
        aOptimistic = sweepCfg.getboolean('Optimistic')
        aurl = sweepCfg['LocalAddr']
        purl = urlparse(aurl)
        ahost = purl.hostname
        aport = purl.port
        anlast = int(sweepCfg['BGP2View'])

    ctx.gap = agap
    if atimeout == 0:
        ctx.to = ctx.gap
    else:
        ctx.to = atimeout

    if aOptimistic: ctx.sweep.swapOptimistic()
    ctx.opt = aOptimistic

    ctx.nlast = anlast
    ctx.sweep = SWEEP(dt.timedelta(minutes= ctx.gap),dt.timedelta(minutes= ctx.to),ctx.opt,anlast*10)

    try:
        app.run(
            host=ahost,
            port=int(aport),
            debug=False
        )
        # while 1:
        #     time.sleep(60)
    except KeyboardInterrupt:
        ctx.sweep.stop()
        ctx.qm.stop()
    print('The End !!!')
