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

from tools.tools import now
from operator import itemgetter

from lxml import etree  # http://lxml.de/index.html#documentation
from lib.bgp import simplifyVars, unSerialize, unSerializeBGP
from lib.QueryManager import QueryManager

from io import StringIO

from sweep import SWEEP, toStr

from flask import Flask, render_template, request, jsonify
# http://flask.pocoo.org/docs/0.12/

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
        self.chglientMode = False
        self.qm = QueryManager(modeStat = False)

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

    rep = '<td><h1>Frequent deduced BGPs</h1><p>('+str(ctx.nlast)+' more frequents)</p>'
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td>'
    ctx.sweep.rankingBGPs.sort(key=itemgetter(1), reverse=True)
    for (bgp, freq, query, lines, precision, recall) in ctx.sweep.rankingBGPs[:ctx.nlast]:
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(bgp):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td><td>%s</td>'%(freq,html.escape(query))
        rep += '</tr>'
    rep += '</table></td>'

    rep += '<td><h1>Frequent Ground Truth Queries</h1><p>('+str(ctx.nlast)+' more frequents)</p>'
    rep += '<table cellspacing="1" border="1" cellpadding="2">'
    rep += '<thead><td>BGP</td><td>Nb Occ.</td><td>Query Exemple</td><td>Avg. Precision</td><td>Avg. Recall</td>'
    ctx.sweep.rankingQueries.sort(key=itemgetter(1), reverse=True)
    for (bgp, freq, query, lines, precision, recall) in ctx.sweep.rankingQueries[:ctx.nlast]:
        rep += '<tr>'
        rep += '<td>'
        for (s,p,o) in simplifyVars(bgp):
            rep += html.escape(toStr(s,p,o))+' . <br/>'
        rep += '</td>'
        rep += '<td>%d</td><td>%s</td><td>%2.3f</td><td>%2.3f</td>'%(freq,html.escape(query), precision/freq, recall/freq)
        rep += '</tr>'
    rep += '</table></td>'

    t += rep + '<tr></table>'

    return rep #'<p>Not implemented</p>'

@app.route('/pr')
def doPR():
    nb = ctx.sweep.stat['nbQueries']
    nbbgp = ctx.sweep.stat['nbBGP']
    if nb>0:
        avgPrecision = ctx.sweep.stat['sumPrecision']/nb
        avgRecall = ctx.sweep.stat['sumRecall']/nb
        avgQual = ctx.sweep.stat['sumQuality']/nb
    else:
        avgPrecision = 0
        avgRecall = 0
        avgQual = 0
    if nbbgp>0 :
        Acuteness = ctx.sweep.stat['sumSelectedBGP'] / nbbgp
    else:
        Acuteness = 0

    return jsonify(result=(avgPrecision,avgRecall))

@app.route('/sweep')
def sweep():
    ctx.cpt += 1
    nb = ctx.sweep.stat['nbQueries']
    nbbgp = ctx.sweep.stat['nbBGP']
    if nb>0:
        avgPrecision = ctx.sweep.stat['sumPrecision']/nb
        avgRecall = ctx.sweep.stat['sumRecall']/nb
        avgQual = ctx.sweep.stat['sumQuality']/nb
    else:
        avgPrecision = 0
        avgRecall = 0
        avgQual = 0
    if nbbgp>0 :
        Acuteness = ctx.sweep.stat['sumSelectedBGP'] / nbbgp
    else:
        Acuteness = 0

    rep = '<h1>Information</h1><table><tr><td>'

    # rep += '<h1>SWEEP parameters</h1>'
    rep += '<table  cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Gap (hh:mm:ss)</td>'
    # rep += '<td>Time out</td>'
    # rep += '<td>Opt</td>'
    rep += '</thead><tr>'
    rep += '<td>%s</td>'%(dt.timedelta(minutes= ctx.gap))
    # rep += '<td>%s</td>'%(dt.timedelta(minutes= ctx.to))
    # rep += '<td>%s</td>'%(str(ctx.opt))
    rep += '</tr></table>'

    rep += '</td><td>'

    # rep += '<h1>Global measures</h1>'
    rep += '<table cellspacing="1" border="1" cellpadding="2"><thead>'
    rep += '<td>Evaluated Queries</td>'
    # rep += '<td>Nb Cancelled Queries</td>'
    # rep += '<td>Nb Empty Queries</td>'
    # rep += '<td>Nb Timeout Queries</td>'
    # rep += '<td>Nb Bad formed Queries</td>'
    # rep += '<td>Nb TPF Client Error</td>'
    # rep += '<td>Nb TPF Client Query Error</td>'
    # rep += '<td>Nb Other query Error</td>'

    rep += '<td>BGP</td><td>TPQ</td>'
    rep += '</thead><tr>'
    rep += '<td>%d / %d</td>'%(nb,ctx.nbQueries)
    # rep += '<td>%d</td>'%(ctx.nbCancelledQueries)
    # rep += '<td>%d</td>'%(ctx.nbEmpty)
    # rep += '<td>%d</td>'%(ctx.nbTO)
    # rep += '<td>%d</td>'%(ctx.nbQBF)
    # rep += '<td>%d</td>'%(ctx.nbClientError)
    # rep += '<td>%d</td>'%(ctx.nbEQ)
    # rep += '<td>%d</td>'%(ctx.nbOther)

    rep += '<td>%d</td><td>%d</td>'%(nbbgp,ctx.nbEntries)
    rep += '</tr></table>'
    rep += '</td><td>'

    # rep += '<table cellspacing="1" border="1" cellpadding="2"><thead>'
    # rep += '<td>Avg Precision</td>'
    # rep += '<td>Avg Recall</td>'
    # # rep += '<td>Avg Quality</td>'
    # # rep += '<td>Acuteness</td>'
    # rep += '</thead><tr>'

    # rep += '<td>%2.3f</td><td>%2.3f</td>'%(avgPrecision,avgRecall)
    # # rep += '<td>%2.3f</td><td>%2.3f</td>'%(avgQual,Acuteness)
    # rep += '</tr></table>'

    rep += '</td></tr></table>\n'




    # rep += '<hr size="2" width="100" align="CENTER" />'

    rep += '<h1>Deduced BGPs</h1><p>('+str(ctx.nlast)+' more recents)</p><table cellspacing="1" border="1" cellpadding="5"  width="100%">\n'
    rep += '<thead><td></td><td>ip</td><td>time</td><td width="35%">bgp</td><td  width="45%">Original query</td><td>Precision</td><td>Recall</td>'
    # rep += '<td>Quality</td>'
    rep += '</thead>\n'
    # for (i,idQ, t,ip,query,bgp,precision,recall) in ctx.sweep.memory[-1*ctx.nlast:] :
    nb = len(ctx.sweep.memory)
    for j in range(min(nb,ctx.nlast)):
        (i,idQ, t,ip,query,bgp,precision,recall) = ctx.sweep.memory[nb-j-1]
        if i==0:
            rep +='<tr><td>'+str(nb-j)+'</td><td>'+bgp.client+'</td><td>'+str(bgp.time)+'</td><td>'
            # for (s,p,o) in simplifyVars([tp for (itp,tp,sm,pm,om) in bgp.tp_set]):
            for (s,p,o) in [tp for (itp,tp,sm,pm,om) in bgp.tp_set]:
                rep += html.escape(toStr(s,p,o))+' . <br/>'
            rep += '</td><td>No query assigned</td><td></td><td></td>'
            # rep += '<td></td>'
            rep += '</tr>'
        else:
            rep +='<tr><td>'+str(nb-j)+'</td><td>'+ip+'</td><td>'+str(t)+'</td><td>'
            if bgp is not None:
                # for (s,p,o) in simplifyVars([tp for (itp,tp,sm,pm,om) in bgp.tp_set]):
                for (s,p,o) in [tp for (itp,tp,sm,pm,om) in bgp.tp_set]:
                    rep += html.escape(toStr(s,p,o))+' . <br/>'
            else:
                rep += 'No BGP assigned !'
            rep += '</td><td>'+idQ+'<br/>'+html.escape(query)+'</td><td>%2.3f</td><td>%2.3f</td>'%(precision,recall)    # .replace('\n','<br/>')
            # rep += '<td>%2.3f</td>'%((precision+recall)/2)
            rep += '</tr>'
    rep += '</table>'
    return rep

# @app.route('/delquery', methods=['post','get'])
# def processDelQuery():
#     if request.method == 'POST':
#         ip = request.remote_addr
#         # data = request.form['data']
#         ctx.sweep.stat['nbQueries'] -= 1
#         ctx.nbCancelledQueries += 1
#         # ctx.nbQueries -= 1
#         return jsonify(result=True)
#     else:
#         print('"delquery" not implemented for HTTP GET')
#         return jsonify(result=False)

@app.route('/run', methods=['get'])
def doRun():
    nbgp = ctx.sweep.nbBGP.value
    nreq = ctx.sweep.nbREQ.value
    return jsonify(result=(nbgp,nreq))

@app.route('/inform', methods=['post','get'])
def processInform():
    if request.method == 'POST':

        ip = request.remote_addr

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
            time = now()# fromISO(q.attrib['time'])

            bgp_list = request.form['bgp_list']
            l = []
            lbgp = etree.parse(StringIO(bgp_list), ctx.parser)
            for x in lbgp.getroot():
                bgp = unSerializeBGP(x)
                l.append(bgp)

            if len(l) == 0:
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

@app.route('/data', methods=['post','get'])
def processData():
    if request.method == 'POST':
        data = request.form['data']
        i = request.form['no']
        time = request.form['time']

        print(i,time)
        # print('Receiving data:',data)

        ip = request.remote_addr
        ip2 = request.form['ip']

        client = ip2 # request.form['ip']
        if client is None:
            client = ip
        elif client in ["undefined","", "undefine"]:
            client = ip
        elif "::ffff:" in client:
            client = client[7:]

        print('DATA - ip-remote:',ip,' ip-post:',ip2, ' choix:',client)

        try:
            tree = etree.parse(StringIO(data), ctx.parser)
            ctx.nbEntries += 1
            for e in tree.getroot():
                if e.tag == 'e':
                    if e[0].get('type')=='var' : e[0].set('val','s')
                    if e[1].get('type')=='var' : e[1].set('val','p')
                    if e[2].get('type')=='var' : e[2].set('val','o')
                    s = unSerialize(e[0])
                    p = unSerialize(e[1])
                    o = unSerialize(e[2])
                    ctx.sweep.putEntry(i,s,p,o,time,client)
                    print('new TPQ : ',toStr(s,p,o))

                elif e.tag == 'd':
                    xs = unSerialize(e[0])
                    xp = unSerialize(e[1])
                    xo = unSerialize(e[2])
                    ctx.sweep.putData(i, xs, xp, xo)
                    # print('new data : ',toStr(xs,xp,xo))

                elif e.tag == 'm':
                    # s = unSerialize(e[0])
                    # p = unSerialize(e[1])
                    # o = unSerialize(e[2])
                    # print('new meta : ',toStr(s,p,o))
                    pass
                else:
                    pass

            ctx.sweep.putEnd(i)

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

def processResults(sweep,list):
    i = 0
    try:
        res = sweep.get()
        while res != None:
            i += 1
            # res.print()
            list.append(res)
            res = sweep.get()
    except KeyboardInterrupt:
        pass

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
    parser.add_argument("--port", type=int, default=5000, dest="port", help="Port (5000 by default)")
    parser.add_argument("--chglientMode", dest="chglientMode", action="store_true", help="Do TPF Client mode")

    args = parser.parse_args()

    ctx.gap = args.gap
    if args.timeout == 0:
        ctx.to = ctx.gap
    else:
        ctx.to = args.timeout

    if args.doOptimistic: ctx.sweep.swapOptimistic()
    ctx.opt = args.doOptimistic
    ctx.chglientMode =  args.chglientMode

    ctx.sweep = SWEEP(dt.timedelta(minutes= ctx.gap),dt.timedelta(minutes= ctx.to),ctx.opt)
    resProcess = mp.Process(target=processResults, args=(ctx.sweep,ctx.list))
    ctx.nlast = args.nlast


    try:
        ctx.sweep.startSession()
        resProcess.start()
        app.run(
            host="0.0.0.0",
            port=int(args.port),
            debug=False
        )
        # while 1:
        #     time.sleep(60)
    except KeyboardInterrupt:
        ctx.sweep.endSession()
        ctx.sweep.stop()
        ctx.qm.stop()
        resProcess.join()
    print('The End !!!')
