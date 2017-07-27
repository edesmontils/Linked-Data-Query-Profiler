# SWEEP Project
SWEEP: a Streaming Web Service to Deduce Basic Graph Patterns from Triple Pattern Fragments.

SWEEP is a tool that allows data providers using a TPF server (see LDF @ linkeddatafragments.org/) to know the queries of their TPF clients, i.e., how their data are used.

## Testing SWEEP
The SWEEP dashboard show the recent deduced BGPs evaluated by a DBpedia's TPF that we setup. 
Il you want to send à SPARQL query to our DBpedia's TPF server you can use any TPF client or use our modified TPF client that allows SWEEP to measure precision and recall of deduced BGPs.

SWEEP Dashboard:
http://sweep.priloo.univ-nantes.fr 

SWEEP TPF Client:
http://tpf-client-sweep.priloo.univ-nantes.fr

Our DBpedia's TPF server:
http://tpf-server-sweep.priloo.univ-nantes.fr

## Installing SWEEP
### Prelude

#### macOS
You need to install [Homebrew](http://brew.sh/).

and then install Python:
```bash
brew install python
```
#### Ubuntu
```
sudo apt-get install python-dev python-setuptools
```

## Dependencies
You need to install dependencies with pip:
- lxml : http://lxml.de/
- RDFLib : https://github.com/RDFLib/rdflib
- networkx : https://github.com/networkx/networkx
- SPARQLWrapper :https://github.com/RDFLib/sparqlwrapper
- iso8601 : https://bitbucket.org/micktwomey/pyiso8601

SWEEP was tested with python3.5 and Python3.6.

## Adapting TPF to SWEEP

TPF server and client available at http://linkeddatafragments.org/software/ are used to test SWEEP. Some changes have to be done on the TPF server. If you want to measure precision and recall, some changes should be done on the client too.

### TPF Server

SWEEP needs the TPF server log to deduce BGPs. So, some modifications should be done on TPF Server code. First, after clonning the project (https://github.com/LinkedDataFragments/Server.js.git) change concerns the file ./bin/ldf-server. Just add the code (from 'Begin SWEEP' to 'End SWEEP') :
```nodejs
...
var configDefaults = JSON.parse(fs.readFileSync(path.join(__dirname, '../config/config-defaults.json'))),
    config = _.defaults(JSON.parse(fs.readFileSync(args[0])), configDefaults),
    port = parseInt(args[1], 10) || config.port,
    workers = parseInt(args[2], 10) || config.workers,
    constructors = {};

//------------------> Begin SWEEP <------------------------
var sweep = config.sweep || '' ;
config.sweep = sweep ;
//------------------> End SWEEP <------------------------

// Start up a cluster master
if (cluster.isMaster) {
...
```
It allows to give the SWEEP URL to the TPF Sever.

Next, install the Request' module and do next changes in ./lib/views/RdfView.js

```nodejs
...

var contentTypes = 'application/trig;q=0.9,application/n-quads;q=0.7,' +
                   'application/ld+json;q=0.8,application/json;q=0.8,' +
                   'text/turtle;q=0.6,application/n-triples;q=0.5,text/n3;q=0.6';

//------------------> Begin SWEEP <------------------------
var http = require('request');
var trace = "";
var cpt = 0;
//------------------> End SWEEP <------------------------


// Creates a new RDF view with the given name and settings
function RdfView(viewName, settings) {
  if (!(this instanceof RdfView))
...

  // Write the triples with a content-type-specific writer
  var self = this,
      writer = /json/.test(settings.contentType) ? this._createJsonLdWriter(settings, response, done)
                                                 : this._createN3Writer(settings, response, done);
  //------------------> Begin SWEEP <------------------------
  cpt += 1;
  settings.cpt =  'e'+process.pid+'-'+cpt ;
  settings.tpList = '';
  //------------------> End SWEEP <------------------------

  settings.writer = writer;

...

  function after()  {
self._renderViewExtensions('After',  settings, request, response, writer.end);

    //------------------> Begin SWEEP <------------------------
    settings.tpList = settings.tpList + '</l>';
    http({
      uri: settings.sweep+"/data",
      method: "POST",
      form: {
        data: settings.tpList,
        no : settings.cpt,
        ip : settings.sweep_ip,
        time : settings.sweep_time
      }
    }, function(error, response, body) {
       console.log('data:',body,error);
    });
    //------------------> End SWEEP <------------------------

}
  function before() {

  //------------------> Begin SWEEP <------------------------
  var ip =
     request.connection.remoteAddress ||
     request.socket.remoteAddress ||
     request.headers['x-forwarded-for'] || request.connection.socket.remoteAddress;

  now = new Date();
  settings.sweep_ip = ip
  settings.sweep_time = now.toJSON()
  trace = '<e>';
  q = settings.query.patternString;
  subject = settings.query.subject;
  predicate = settings.query.predicate;
  object = settings.query.object;
  var s= ( subject=== undefined ? '<s type="var"/>' : toIRI(subject,'s'));
  var p= ( predicate=== undefined ? '<p type="var"/>' : toIRI(predicate,'p'));
  var o= ( object=== undefined ? '<o type="var"/>' : toIRI(object,'o'));
  trace += s+p+o+'</e>';
  settings.tpList = '<l>'+trace;
  //------------------> End SWEEP <------------------------

...

    data: function (s, p, o, g) {
      writer.addTriple(s, p, o, supportsGraphs ? g : null);

      //------------------> Begin SWEEP <------------------------
      if (o === undefined)
        tp = '<d>'+toIRI(s.subject,'s')+''+toIRI(s.predicate,'p')+''+toIRI(s.object,'o')+'</d>';
        else tp = '<d>'+toIRI(s,'s')+' '+toIRI(p,'p')+' '+toIRI(o,'o')+'</d>';
      settings.tpList = settings.tpList + tp
      //------------------> End SWEEP <------------------------

    },
    // Adds the metadata triple to the output
    meta: function (s, p, o) {
      // Relate the metadata graph to the data
      if (supportsGraphs && !metadataGraph) {
        metadataGraph = settings.metadataGraph;
        writer.addTriple(metadataGraph, primaryTopic, settings.fragmentUrl, metadataGraph);
      }
      // Write the triple
      if (s && p && o && !N3.Util.isLiteral(s)) {
        writer.addTriple(s, p, o, metadataGraph);
        //------------------> Begin SWEEP <------------------------
        if (o === undefined)
          tp = '<m>'+toIRI(s.subject,'s')+''+toIRI(s.predicate,'p')+''+toIRI(s.object,'o')+'</m>';
          else tp = '<m>'+toIRI(s,'s')+' '+toIRI(p,'p')+' '+toIRI(o,'o')+'</m>';
        settings.tpList = settings.tpList + tp
        //------------------> End SWEEP <------------------------
      }
    },

...

//------------------> Begin SWEEP <------------------------
function toIRI(s,p) {return s[0]   !== '_' ? (!N3.Util.isLiteral(s) ? '<'+p+' type="iri" val="'+s.replace(/&/gi, "&amp;")+'"/>' : '<'+p+' type="lit"><![CDATA['+s+']]></'+p+'>') : '<'+p+' type="bkn" val="'+s+'"/>';}
//------------------> End SWEEP <------------------------

module.exports = RdfView;

```
These modifications allow the TPF Server to send to SWEEP the execution log. These modifications are enough to run SWEEP. If you want to measure precision and recall, in the next we describe the modificaitons to apply to the TPF client.

### TPF Client

Let's take the ./bin/ldf-client file. After clonning the project (https://github.com/LinkedDataFragments/Client.js.git), do next modifications :
```nodejs
// Parse and initialize configuration
var configFile = args.c ? args.c : path.join(__dirname, '../config-default.json'),
    config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' })),
    queryFile = args.f || args.q || args._.pop(),
    startFragments = args._,
    query = args.q || (args.f || fs.existsSync(queryFile) ? fs.readFileSync(queryFile, 'utf8') : queryFile),
    mimeType = args.t || 'application/json',
    datetime = args.d || config.datetime;

//------------------> Begin SWEEP <------------------------
var sweep = args.s || '' ;
config.sweep = sweep ;
//------------------> End SWEEP <------------------------

// parse memento datetime
if (datetime)
  config.datetime = datetime === true ? new Date() : new Date(datetime);

```

Then, lfd-client command line allows to specify the SWEEP server (with '-s').
Next, apply next modifications to ./lib/sparql/SparqlIterator.js :
```nodejs
...
  // Transform the query into a cascade of iterators
  try {
    // Parse the query if needed
    if (typeof query === 'string') {

      //------------------> Begin SWEEP <------------------------
      if (options.sweep != ''){      
        now = new Date();
        trace = '<query time="'+now.toJSON()+'"><![CDATA['+query+']]></query>'
        http({    
          uri: options.sweep+"/query",
          method: "POST",
          form: {
            data: trace, no:'ldf-client', 'bgp_list': '<l/>'
          }
        }, function(error, response, body) {
        }); 
      }
      //------------------> End SWEEP <------------------------

      query = new SparqlParser(options.prefixes).parse(query);
    }
...
```

This code sends the query to SWEEP. This allows SWEEP to alculate precision and recall.

## Running SWEEP

From `~/SWEEP` run the comand to run SWEEP:
```bash
nohup python3.5 sweep-streamWS.py -g 0.250 -to 0.2 -l 20 --port 5000 &> resSWEEP &
```
The Dashboard is now available at : http:127.0.0.1:5000

For the (modified) TPF Server, change the config file to specify the SWEEP server and datasources :

```bash
{
  "title": "My Linked Data Fragments server",
  "port": 5001,
  "workers": 8,
  "sweep" : "http://127.0.0.1:5002",
...
  "datasources": {
   "dbpedia": {
      "title": "DBpedia",
      "type": "HdtDatasource",
      "description": "DBpedia 3.8 backend",
      "settings": { "file": "dbpedia-3.8.hdt" }    
   },
   ...
  },
...
```

Then run the server :
```bash
./bin/ldf-server config/config-dbp.json
```

Finally, the SWEEP client to test SWEEP can be run :
```bash
nohup python3.5 qsim-WS.py --sweep http://127.0.0.1:5000 -s http://127.0.0.1:5001 -c /.../bin/ldf-client --port 5002 -v -g 0.25 &> resQsim-WS &
```
The client is now available at : http:127.0.0.1:5002

#### Command lines

```
$ python3.6 sweep-streamWS.py -h
usage: sweep-streamWS.py [-h] [-g GAP] [-to TIMEOUT] [-o] [-l NLAST]
                         [--port PORT] [--chglientMode]

Linked Data Query Profiler (for a modified TPF server)

optional arguments:
  -h, --help            show this help message and exit
  -g GAP, --gap GAP     Gap in minutes (60 by default)
  -to TIMEOUT, --timeout TIMEOUT
                        TPF server Time Out in minutes (0 by default). If '-to
                        0', the timeout is the gap.
  -o, --optimistic      BGP time is the last TP added (False by default)
  -l NLAST, --last NLAST
                        Number of last BGPs to view (10 by default)
  --port PORT           Port (5002 by default)
  --chglientMode        Do TPF Client mode
```


```
$ python3.6 qsim-WS.py -h
usage: qsim-WS.py [-h] [--sweep SWEEP] [-s TPFSERVER] [-c TPFCLIENT] [-v]
                  [-g GAP] [-to TIMEOUT] [--port PORT]

Linked Data Query Profiler (for a modified TPF server)

optional arguments:
  -h, --help            show this help message and exit
  --sweep SWEEP         SWEEP ('http://127.0.0.1:5002' by default)
  -s TPFSERVER, --server TPFSERVER
                        TPF Server ('http://127.0.0.1:5000' by default)
  -c TPFCLIENT, --client TPFCLIENT
                        TPF Client ('...' by default)
  -v, --valid           Do precision/recall
  -g GAP, --gap GAP     Gap in minutes (60 by default)
  -to TIMEOUT, --timeout TIMEOUT
                        TPF Client Time Out in minutes (no timeout by
                        default).
  --port PORT           Port (5002 by default)
```

