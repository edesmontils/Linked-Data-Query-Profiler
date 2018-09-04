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

# from sweep import SWEEP, toStr

from flask import Flask, render_template, request, jsonify
# http://flask.pocoo.org/docs/0.12/

from urllib.parse import urlparse, unquote_plus
from configparser import ConfigParser, ExtendedInterpolation

import requests as http

import json
from tools.Socket import SocketClient, MsgProcessor

def toStr(s, p, o):
    return serialize2string(s)+' '+serialize2string(p)+' '+serialize2string(o)

class Context(object):
    """docstring for Context"""
    def __init__(self):
        super(Context, self).__init__()
        self.cpt = 0
        self.to = 0.0
        self.gap = 0.0
        self.opt = False
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
    return render_template('index-sweep.html',nom_appli="SWEEP Dashboard", version="0.3")

@app.route('/bestof')
def bo():
    t = '<table cellspacing="50"><tr>'

    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    (memDuration, r) = sweep.sendMsg( { 'path' : '/bestof-1'} )
    nb = len(r)
    rep = '<td><h1>Frequent deduced BGPs</h1> <p>(short term memory : %s ; %s more frequents)</p>'%(memDuration,nb)
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td>'
    for (sbgp, freq, query) in r:
        rep += '<tr><td>'
        for tp in sbgp : rep += html.escape(tp)+' <br/>'
        rep += '</td><td>%d</td><td>%s</td>'%( freq, html.escape(query) )
        rep += '</tr>'
    rep += '</table></td>'

    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    r = sweep.sendMsg( { 'path' : '/bestof-2'} )
    nb = len(r)
    rep += '<td><h1>Frequent deduced BGPs [MAA05]</h1><p>(long term memory ; '+str(nb)+' more frequents)</p>'
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td>'
    for e in r:
        (l, c) = e
        rep += '<tr><td>'
        for tp in l : rep += html.escape(tp)+' <br/>'
        rep += '</td><td>%d</td></tr>'%c
    rep += '</table></td>'


    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    (memDuration, r) = sweep.sendMsg( { 'path' : '/bestof-3'} )
    rep += '<td><h1>Frequent Ground Truth Queries</h1> <p>(short term memory : %s ; %s more frequents)</p>'%(memDuration,nb)
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td><td>Avg. Precision</td><td>Avg. Recall</td>'
    for (sbgp, freq, query, precision, recall) in r:
        rep += '<tr><td>'
        for tp in sbgp : rep += html.escape(tp)+' <br/>'
        rep += '</td><td>%d</td><td>%s</td><td>%2.3f</td><td>%2.3f</td>'%(freq,html.escape(query), precision/freq, recall/freq)
        rep += '</tr>'
    rep += '</table></td>'

    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    r = sweep.sendMsg( { 'path' : '/bestof-4'} )
    nb = len(r)
    rep += '<td><h1>Frequent Ground Truth Queries [MAA05]</h1> <p>(long term memory ; %s more frequents)</p>'%str(nb)
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td>'
    for e in r:
        (l, c) = e
        rep += '<tr><td>'
        for tp in l : rep += html.escape(tp)+' <br/>'
        rep += '</td><td>%d</td></tr>'%c
    rep += '</table></td>'    

    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    r = sweep.sendMsg( { 'path' : '/bestof-5'} )
    rep += '<td><h1>P/R for users </h1>'
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>Code</td><td>Nb queries</td><td>Precision</td><td>Recall</td>'
    for (ip,nb,precision,recall) in r :
        rep += '<tr><td>%s</td><td>%d</td><td>%2.3f</td><td>%2.3f</td></tr>'%(ip,nb,precision,recall)
    rep += '</table></td>'

    t += rep + '<tr></table>'

    return rep

@app.route('/sweep')
def sweep():
    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    (nb, nbbgp, gap, nbQueries, nbEntries, nlast, l ) = sweep.sendMsg( { 'path' : '/sweep'} )

    rep = '<h1>Information</h1><table><tr><td>'

    rep += '<table  cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Gap (hh:mm:ss)</td>'
    rep += '</thead><tr>'
    rep += '<td>%s</td>'%(gap)
    rep += '</tr></table>'

    rep += '</td><td>'

    rep += '<table cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Evaluated Queries</td>'

    rep += '<td>BGP</td><td>TPQ</td>'
    rep += '</thead><tr>'
    rep += '<td>%d / %d</td>'%(nb,nbQueries)

    rep += '<td>%d</td><td>%d</td>'%(nbbgp,nbEntries)
    rep += '</tr></table>'
    rep += '</td><td>'

    rep += '</td></tr></table>\n'

    rep += '<h1>Deduced BGPs</h1><p>('+nlast+' more recents)</p><table cellspacing="1" border="1" cellpadding="5"  width="100%">\n'
    rep += '<thead><td></td><td>ip</td><td>time</td><td width="35%">bgp</td><td  width="45%">Original query</td><td>Precision</td><td>Recall</td>'
    rep += '</thead>\n'
    for i in l:
        (nbmj, ip, t, sbgp, idQ, queryCode, query, precision, recall) = i
        if query != "No query assigned" :
            squery = idQ+' | '+queryCode+'<br/>'+html.escape(query)
        else :
            squery = "No query assigned"
        rep +='<tr><td>'+nbmj+'</td><td>'+ip+'</td><td>'+t+'</td><td>'
        for tp in sbgp : rep += html.escape(tp)+' <br/>'
        rep += '</td><td>'+squery
        if (precision is not None) and (recall is not None) :
            rep += '</td><td>%2.3f</td><td>%2.3f</td>'%(precision,recall)+'</tr>'
        else :
            rep += '</td><td></td><td></td></tr>'
    rep += '</table>'
    return rep

@app.route('/run', methods=['get'])
def doRun():
    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    res = sweep.sendMsg( { 'path' : '/run'} )
    return jsonify(result=(res[0], res[1]))

@app.route('/pr')
def doPR():
    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    res = sweep.sendMsg( { 'path' : '/run'} )
    nb = res[2]
    if nb>0:
        avgPrecision = res[3]/nb
        avgRecall = res[4]/nb
    else:
        avgPrecision = 0
        avgRecall = 0

    return jsonify(result=(avgPrecision,avgRecall))

@app.route('/mentions')
def mentions():
    s = """
        
    """
    return s

@app.route("/save")
def save():
    sweep = SocketClient(port=5004, msgSize = 2048, ClientMsgProcessor = MsgProcessor() )
    res = sweep.sendMsg({ 'path' : '/save'})
    return jsonify(result=True)

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

    ctx.opt = aOptimistic

    ctx.nlast = anlast
    # ctx.sweep = SWEEP(dt.timedelta(minutes= ctx.gap),dt.timedelta(minutes= ctx.to),ctx.opt,anlast*10)

    try:
        app.run(
            host=ahost,
            port=int(aport),
            debug=False
        )
    except KeyboardInterrupt:
        pass
    finally:
        pass
        # # print('Terminaison')
        # # print('The End !!!')
