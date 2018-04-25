import networkx as nx
import networkx.algorithms.isomorphism as iso
from pprint import pprint

def equals(g1, g2):
    #---
    assert isinstance(g1, nx.Graph) and isinstance(g2, nx.Graph)
    #---
    return nx.algorithms.isomorphism.DiGraphMatcher(g1, g2, 
        #node_match=nm, 
        node_match=iso.categorical_node_match('type', ''),
        #edge_match=em
        edge_match=iso.categorical_multiedge_match('prop','')
        ).is_isomorphic()

# Ne fonctionne pas. Déposé un ticket sur le projet NetworkX 
# car j'ai des doutes sur le bon fonctionnement de "subgraph_is_isomorphic"
# voir le code en bas.
# A suivre...
def isSubGraphOf(g1, g2):
    #---
    assert isinstance(g1, nx.Graph) and isinstance(g2, nx.Graph)
    #---
    GM = nx.algorithms.isomorphism.DiGraphMatcher(g2, g1, 
                                     #node_match=nm,
                                     node_match=iso.categorical_node_match('type', ''), 
                                     #edge_match=em
                                     edge_match=iso.categorical_multiedge_match('prop','type')
                                     )
    if GM.subgraph_is_isomorphic(): return GM.mapping
    else: return None

def isSGO(g1, g2):
    GM = nx.algorithms.isomorphism.DiGraphMatcher(g2, g1 ,
                                     #node_match=nm2,
                                     #node_match=iso.categorical_node_match('type', ''), 
                                     #edge_match=em2,
                                     edge_match=iso.categorical_multiedge_match('prop','type')
                                     )
    if GM.subgraph_is_isomorphic(): return GM.mapping
    else: return None

def f():
    print("main")

    g6 = nx.MultiDiGraph()
    g6.add_edges_from([ (1,2,dict(prop='type')), 
                        (1,3,dict(prop='manage')),
                        #(1,4,dict(prop='manage')),
                        (1,3,dict(prop='knows')),
                        (3,2,dict(prop='type')),
                        (4,2,dict(prop='type'))  
                      ])
    print('g6')
    for e in g6.edges(data=True):
        pprint(e)

    g7 = nx.MultiDiGraph()
    g7.add_edges_from([ (5,6,dict(prop='type')), 
                        (5,7,dict(prop='knows')),
                        (7,6,dict(prop='type')),
                        (8,6,dict(prop='type')),
                        (9,6,dict(prop='type')),
                        (5,10,dict(prop='bP'))  
                      ])
    print('g7')
    for e in g7.edges(data=True):
        pprint(e)

    map = isSGO(g6,g7)
    if map is not None: print('g6 in g7 : ', map)
    else: print('g6 not in g7') 

    map = isSGO(g7,g6)
    if map is not None: print('g7 in g6 : ', map)
    else: print('g7 not in g6')

f()