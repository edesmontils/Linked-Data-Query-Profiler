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

import datetime as dt
import argparse
from tools.Endpoint import TPFEP,TPFClientError, TimeOut, QueryBadFormed, EndpointException
from tools.tools import  now, date2str

from lib.QueryManager import  QueryManager
from lib.bgp import serializeBGP2str

from flask import Flask, render_template, request, jsonify
# http://flask.pocoo.org/docs/0.12/
# from flask_cas import CAS, login_required

from lxml import etree  # http://lxml.de/index.html#documentation

import requests as http
# http://docs.python-requests.org/en/master/user/quickstart/

from urllib.parse import urlparse
from configparser import ConfigParser, ExtendedInterpolation

class Context(object):
    """docstring for Context"""
    def __init__(self):
        super(Context, self).__init__()
        self.sweep = 'http://127.0.0.1:5002'
        self.tpfc = TPFEP(service = 'http://localhost:5000/lift') 
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
        self.qm = QueryManager(modeStat = False)
        self.doPR = False
        self.lastProcessing = -1
        self.gap = 60
        self.addr = ''

    def setLDQPServer(self, host):
        self.sweep = host

    def setTPFClient(self,tpfc):
        self.tpfc = tpfc
        
ctx = Context()

#==================================================

# Initialize the Flask application
app = Flask(__name__)
 
    # //authentification CAS
    # define("C_CASServer","cas-ha.univ-nantes.fr") ;
    # define("C_CASPort",443) ;
    # define("C_CASpath","/esup-cas-server") ;

# set the secret key.  keep this really secret:
app.secret_key = '\x0ctD\xe3g\xe1XNJ\x86\x02\x03`O\x98\x84\xfd,e/5\x8b\xd1\x11'

# cas = CAS(app)
# app.config['CAS_SERVER'] = 'https://cas-ha.univ-nantes.fr:443' 
# app.config['CAS_PORT'] = 443
# app.config['CAS_PATH'] = '/esup-cas-server'
# app.config['CAS_AFTER_LOGIN'] = 'route_root'


@app.route('/')
# @login_required
def index():
    return render_template(
        'index-qsim.html',
        # username = cas.username,
        # display_name = cas.attributes['cas:displayName'],
        nom_appli=ctx.name, version=ctx.version, listeNoms=ctx.listeNoms
    ) 

@app.route('/liste_noms')
def liste_noms():
    return jsonify(result=ctx.listeNoms)

@app.route('/liste_bases')
def liste_bases():
    return jsonify(result=ctx.listeBases)

@app.route('/end')
def end():
    return "<p>Bases purgées...</p>"

@app.route('/ex/<datasource>')
def ex(datasource):
    d = []
    parser = etree.XMLParser(recover=True, strip_cdata=True)
    if datasource=='dbpedia3.8':
        tree = etree.parse('tests/test4.xml', parser)
    elif datasource=='lift':
        tree = etree.parse('tests/test1.xml', parser)
    else:
        return jsonify(result = d)
    #---
    dtd = etree.DTD('http://documents.ls2n.fr/be4dbp/log.dtd')
    assert dtd.validate(tree), 'non valide au chargement : %s' % (
        dtd.error_log.filter_from_errors()[0])
    #---
    # print('DTD valide !')

    nbe = 0 # nombre d'entries traitées
    for entry in tree.getroot():
        if entry.tag == 'entry':
            nbe += 1
            valid = entry.get("valid")
            if valid is not None :
                if valid in ['TPF','EmptyTPF'] :
                    # print('(%d) new entry to add ' %nbe)
                    rep = ''
                    for x in entry :
                        if x.tag == 'bgp':
                            if len(x)>0:
                                rep += etree.tostring(x).decode('utf-8')
                    # print(rep)
                    d.append( (entry.find('request').text , datasource, rep) )
                # else: print('(%d) entry not loaded : %s' % (n,valid))
            # else: print('(%d) entry not loaded (not validated)' % n)   
    return jsonify(result = d)

@app.route('/news')
def news():
    listeMessages = ctx.tree.getroot().findall('listeMessages/message')
    d = list()
    for message in listeMessages:
        r = dict()
        titre = message.get('titre')
        date = message.get('date')
        auteur = message.get('auteur')
        r['titre'] = titre
        r['post'] = "-> Le "+date+" par "+auteur
        s = ''
        for cont in message: s+= etree.tostring(cont, encoding='utf8').decode('utf8')
        r['s'] = s
        d.append(r)
    return jsonify(result=d)

@app.route('/mentions')
def mentions():
    m = ctx.tree.getroot().find('mentions')
    s = ''
    for cont in m: 
        if cont.text is not None: s+= etree.tostring(cont, encoding='utf8').decode('utf8')
    return s

@app.route('/apropos')
def apropos():
    m = ctx.tree.getroot().find('aPropos')
    s = ''
    for cont in m:
        if cont.text is not None: s+= etree.tostring(cont, encoding='utf8').decode('utf8')
    return s

@app.route('/help')
def help():
    m = ctx.tree.getroot().find('aides')
    s = ''
    for cont in m:
        if cont.text is not None: s+= etree.tostring(cont, encoding='utf8').decode('utf8')
    return s

@app.route('/envoyer', methods=['post'])
def envoyer():
    query = request.form['requete']
    datasource = request.form['base']
    bgp_list = request.form['bgp_list']
    print('Recieved BGP:',bgp_list)
    if bgp_list is '' :
        bgp_list = ''
    ip = request.remote_addr
    s=treat(query,bgp_list,ip,datasource)
    tab = doTab(s)
    d = dict({'ok':s != 'Error','val':tab})
    return jsonify(result=d)

@app.route('/liste/bd/<datasource>')
def liste(datasource):
    ip = request.remote_addr
    # print(datasource, )
    s=treat("select * where{?s ?p ?o} limit 50",'',ip,datasource)
    tab = doTab(s)
    d = dict({'ok':s != 'Error','val':tab})
    return jsonify(result=d)    
    # return "<p>"+soumettre+"Pas de requête ou/et de base proposée !</p>"

def doTab(s):
    if len(s)>0:
        m = s[0]
        if type(m) == str :
            tab = '<p>%s</p>'%s
        else:
            tab = '<table cellspacing="1" border="1" cellpadding="3">\n<thead><th></th>'
            for (var,val) in m.items():
                tab += '<th>'+str(var)+'</th>'
            tab += '</thead>\n'
            i = 0
            for m in s:
                i +=1
                tab += '<tr><td>%d</td>'%i
                for (var,val) in m.items():
                    tab += '<td>'+str(val)+'</td>'
                tab += '</tr>\n'
            tab += '</table><br/>'
    else:
        tab = '<p> Empty </p>\n'
    if ctx.lastProcessing > ctx.gap :
        tab += '(%s sec.)'%(ctx.lastProcessing.total_seconds()),' The gap (%s) is exceeded.'%ctx.gap.total_seconds()
    else:
        tab += '(%s sec.)'%(ctx.lastProcessing.total_seconds())
    tab += '<br/>'
    return tab

# '<l><bgp><tp><s type="var" val="s"/><p type="iri" val="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"/><o type="var" val="o"/></tp></bgp></l>'

def treat(query,bgp_list,ip,datasource):
    try:
        ctx.nbQuery += 1
        nbe = ctx.nbQuery
        doPR = ctx.doPR
        no = 'qsim-WS-'+str(ip)+'-'+str(ctx.nbQuery)

        if bgp_list == '':
            (bgp,nquery) = ctx.qm.extractBGP(query)
            query = nquery
            bgp_list = serializeBGP2str(bgp) 

        # mess = '<query time="'+date2str(now())+'" client="'+str(ip)+'" no="'+no+'"><![CDATA['+query+' ]]></query>'
        mess = '<query time="'+date2str(now())+'" no="'+no+'"><![CDATA['+query+' ]]></query>'

        url = ctx.sweep+'/query'
        print('(%d)'%nbe,'query:',mess)

        bgp_list = '<l>'+bgp_list+'</l>'
        print(bgp_list)

        #s = http.post(url,data={'data':mess, 'no':no, 'bgp_list': bgp_list})
        
        # print('res:',s.json()['result'])
        # res=  ctx.listeSP[datasource].query(query) # ctx.tpfc.query(query)
        # pprint(res)
        # print(type(res))
        try:
            before = now()
            res=  ctx.listeSP[datasource].query(query)#,' -i '+bgp_list+' ')
            after = now()
            ctx.lastProcessing = after - before
            # print('(%d)'%nbe,':',rep)
            if res == []:
               print('(%d, %s sec.)'%(nbe,ctx.lastProcessing.total_seconds()),"Empty query !!!")
               url = ctx.sweep+'/inform'
               s = http.post(url,data={'data':mess,'errtype':'Empty', 'no':no})
            else: 
                print('(%d, %s sec.)'%(nbe,ctx.lastProcessing.total_seconds()),': [...]')#,res)
            if ctx.lastProcessing > ctx.gap :
                print('(%d, %s sec.)'%(nbe,ctx.lastProcessing.total_seconds()),'!!!!!!!!! hors Gap (%s) !!!!!!!!!'%ctx.gap.total_seconds())

        except TPFClientError as e :
            print('(%d)'%nbe,'Exception TPFClientError : %s'%e.__str__())
            if doPR:
                url = ctx.sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'CltErr', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
            res='Error'
        except TimeOut as e :
            print('(%d)'%nbe,'Timeout :',e)
            if doPR:
                url = ctx.sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'TO', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result'])   
            res='Error'     
        except QueryBadFormed as e:
            print('(%d)'%nbe,'Query Bad Formed :',e)
            if doPR:
                url = ctx.sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'QBF', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
            res='Error:'+e.__str__()
        except EndpointException as e:
            print('(%d)'%nbe,'Endpoint Exception :',e)
            if doPR:
                url = ctx.sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'EQ', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result']) 
            res='Error'
        except Exception as e:
            print('(%d)'%nbe,'Exception execution query... :',e)
            if doPR:
                url = ctx.sweep+'/inform'
                s = http.post(url,data={'data':mess,'errtype':'Other', 'no':no})
                print('(%d)'%nbe,'Request cancelled : ',s.json()['result'])
            res='Error'
    except Exception as e:
        print('Exception',e)
        res='Error:'+e.__str__()
    finally:
        return res

#==================================================
#==================================================
#==================================================
 
# launch : python3.6 ldqp-WS.py 
# example to request : curl -d 'query="select * where {?s :p1 ?o}"' http://127.0.0.1:8090/_add_query

TPF_SERVEUR = 'http://127.0.0.1:5000'
TPF_CLIENT = '/Users/desmontils-e/Programmation/TPF/Client.js-master/bin/ldf-client'
SWEEP_SERVEUR = 'http://127.0.0.1:5002'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linked Data Query Profiler (for a modified TPF server)')
    # parser.add_argument('files', metavar='file', nargs='+',help='files to analyse')

    parser.add_argument("--sweep", default=SWEEP_SERVEUR, dest="sweep", help="SWEEP ('"+str(SWEEP_SERVEUR)+"' by default)")
    parser.add_argument("-s","--server", default=TPF_SERVEUR, dest="tpfServer", help="TPF Server ('"+TPF_SERVEUR+"' by default)")
    parser.add_argument("-c", "--client", default=TPF_CLIENT, dest="tpfClient", help="TPF Client ('...' by default)")
    # parser.add_argument("-t", "--time", default='', dest="now", help="Time reference (now by default)")
    parser.add_argument("-v", "--valid", default='', dest="valid", action="store_true", help="Do precision/recall")
    parser.add_argument("-g", "--gap", type=float, default=60, dest="gap", help="Gap in minutes (60 by default)")
    parser.add_argument("-to", "--timeout", type=float, default=None, dest="timeout",help="TPF Client Time Out in minutes (no timeout by default).")
    parser.add_argument("--host", default="127.0.0.1", dest="host", help="host ('127.0.0.1' by default)")
    parser.add_argument("--port", type=int, default=5002, dest="port", help="Port (5002 by default)")

    parser.add_argument("-f", "--config", default='', dest="cfg", help="Config file")

    args = parser.parse_args()

    if (args.cfg == '') :
        asweep = args.sweep
        atpfServer = args.tpfServer
        atpfClient = args.tpfClient
        ahost = args.host
        aport = args.port
        agap = args.gap
        avalid = args.valid
        ato = args.timeout
        aurl = 'http://'+ahost+":"+str(aport)
    else :
        cfg = ConfigParser(interpolation=ExtendedInterpolation())
        r = cfg.read(args.cfg)
        if r == [] :
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

    ctx.setLDQPServer(asweep)
    # http://localhost:5000/lift : serveur TPF LIFT (exemple du papier)
    # http://localhost:5001/dbpedia_3_9 server dppedia si : ssh -L 5001:172.16.9.3:5001 desmontils@172.16.9.15
    ctx.gap = dt.timedelta(minutes= agap)
    XMLparser = etree.XMLParser(recover=True, strip_cdata=True)
    ctx.tree = etree.parse('config.xml', XMLparser)
    #---
    dtd = etree.DTD('config.dtd')
    assert dtd.validate(ctx.tree), '%s non valide au chargement : %s' % (
        file, dtd.error_log.filter_from_errors()[0])
    #---
    lb = ctx.tree.getroot().findall('listeBases/base_de_donnee')
    for l in lb :
        f = l.find('fichier')
        ref = l.find('référence')
        if ref.text is None: ref.text=''
        print('Configure ',l.get('nom'), ' in ',atpfServer+'/'+f.get('nom'))
        sp = TPFEP(service = atpfServer, dataset= f.get('nom'), clientParams= '-s '+asweep )
        sp.setEngine('http_proxy= '+atpfClient)
        ctx.listeBases[l.get('nom')] = {'fichier':f.get('nom'),'prefixe':f.get('prefixe'),'référence':ref.text,
                                        'description':etree.tostring(l.find('description'), encoding='utf8').decode('utf8'),
                                        'tables':[]}
        ctx.listeSP[l.get('nom')] = sp
    ctx.listeNoms = list(ctx.listeBases.keys())
    ctx.version = ctx.tree.getroot().get('version')
    ctx.name = ctx.tree.getroot().get('name')
    if ctx.tree.getroot().get('debug') == 'false': ctx.debug = False
    else: ctx.debug = True
    if avalid:
        ctx.doPR = True
    try:
        ctx.addr = aurl
        print('Running qsim-WS on ' , ctx.addr)
        app.run(
            host=ahost,
            port=int(aport),
            debug=True
        )
    except KeyboardInterrupt: 
        pass
    finally:   
        # ctx.qm.stop()
        pass
    print('Fin')

