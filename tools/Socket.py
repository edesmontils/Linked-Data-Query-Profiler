#!/usr/bin/env python3.6
# coding: utf8
"""
Class to process statistics in a parallel context
"""
#    Copyright (C) 2017 by
#    Emmanuel Desmontils <emmanuel.desmontils@univ-nantes.fr>
#    Patricia Serrano-Alvarado <patricia.serrano-alvarado@univ-nantes.fr>
#    All rights reserved.
#    GPL v 2.0 license.

import sys
import socket
import json 

#==================================================

class Socket(object):
    """docstring for Socket"""
    def __init__(self, s=None, port=5005, host = '127.0.0.1', msgSize = 128):
        super(Socket, self).__init__()
        self.msgSize = msgSize
        if s is None:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            (self.cl_ip, self.cl_port) = ('',0)
            (self.loc_ip,self.loc_port) = (host, port)
        else:
            self.s = s
            (self.cl_ip, self.cl_port) = self.s.getpeername()
            (self.loc_ip,self.loc_port) = self.s.getsockname()
        # print ('Socket created')
        self.endTag = b'/[END-SOCKET-ED]/'
        self.lenEndTag = len(self.endTag)

    def close(self):
        self.s.close()

    def send(self,cdc):
        #---
        # assert type(cdc) == 'bytes', 's must be a byte'
        self.s.send(cdc)

    def sendall(self,cdc):
        #---
        # assert type(cdc) == 'bytes', 's must be a byte'
        self.s.sendall(cdc)

    def recv(self):
        return self.s.recv(self.msgSize)

    def locAddr(self):
        return self.loc_ip + ':' + str(self.loc_port)

    def clAddr(self):
        return self.cl_ip + ':' + str(self.cl_port)

    def getMsgSize(self):
        m = self.recv()
        size = int( m.decode('utf-8') )
        self.send(b"1")
        return size

    def getMsgBody(self, size) :
        mesg = b""
        m = self.recv()
        while True :
            mesg += m
            # print("reception de %s (%d/%d)"%(m,len(mesg),size))
            if len(mesg) >= size:
                break
            else:
                m = self.recv()
        if len(mesg) == size: 
            return mesg.decode('utf-8')
        else: return None

    def getMsgBody2(self) :
        mesg = b""
        # self.s.settimeout(1)
        try:
            m = self.recv()
            while m != b'' :
                mesg += m
                # print("reception de %s (%d)"%(m,len(mesg)))
                if mesg[- self.lenEndTag:] == self.endTag :
                    # print('End tag founded')
                    mesg = mesg[:- self.lenEndTag]
                    break
                else :
                    m = self.recv()
        except KeyboardInterrupt :
            pass
        except socket.error as se:
            print('socket error ',se)
        finally :
            pass
        if mesg != b'': 
            return mesg.decode('utf-8')
        else: return None

    def getMsg(self):
        size = self.getMsgSize()
        mesg = self.getMsgBody(size)
        return(mesg)

    def getMsg2(self):
        mesg = self.getMsgBody2()
        return(mesg)


    def putMsg(self, msg) :
        msg = msg.encode()
        l =  len(msg) 
        self.send(b"%d"%l)
        msgServeur = self.recv()
        if msgServeur != b"1" :
            print("pb d'échange (1)")
            rep = False
        else: 
            self.sendall(msg)
            rep = True
        return rep

    def putMsg2(self, msg) :
        msg = msg.encode()
        rep = self.sendall(msg + self.endTag)
        # self.send(b'')
        return rep

#==================================================

class SocketServer(Socket) :
    def __init__(self, s=None, port=5005, host = '127.0.0.1', msgSize = 128, ServerMsgProcessor = None):
        super(SocketServer, self).__init__(s,port,host,msgSize)
        if ServerMsgProcessor is not None :
            assert isinstance(ServerMsgProcessor,MsgProcessor), 'ServerMsgProcessor must be a MsgProcessor'
            self.processor = ServerMsgProcessor
        self.bind()

    def bind(self):
        #Bind socket to local host and port
        try:
            self.s.bind((self.loc_ip, self.loc_port))
        except socket.error:
            print ('Bind failed.')
            sys.exit()   
        # print ('Socket bind complete')

    def accept(self):
        (conn, (ip, port)) = self.s.accept()
        (self.processor.cl_ip,self.processor.cl_port) = (ip, port)
        (self.processor.loc_ip,self.processor.loc_port) = (self.loc_ip,self.loc_port)
        return Socket(conn,msgSize = self.msgSize)

    def run(self) :
        try :
            while True :
                self.s.listen(5)
                connexion = self.accept()
                print ("Client connecté, adresse %s" % connexion.clAddr())
                mesg = connexion.getMsg()
                # print("Server received : %s" % mesg)

                if mesg:
                    if self.processor is not None:
                        rep = self.processor.processIn(mesg)
                        # print(rep)
                        if rep is not None :
                            res = connexion.putMsg(rep)
                        else: 
                            res = connexion.putMsg("1")
                    else: 
                        res = connexion.putMsg("1")
                else : 
                    print("Pb de réception")
                    res = connexion.putMsg("0")

                connexion.close()

        except KeyboardInterrupt :
            pass
        except socket.error:
            pass
        finally :
            pass

    def run2(self) :
        try :
            while True :
                self.s.listen(5)
                connexion = self.accept()
                print ("Client connecté, adresse %s" % connexion.clAddr())
                mesg = connexion.getMsg2()
                # print("Server received : %s" % mesg)
                if mesg:
                    if self.processor is not None:
                        rep = self.processor.processIn(mesg)

                connexion.close()

        except KeyboardInterrupt :
            pass
        except socket.error:
            pass
        finally :
            pass

#==================================================

class MsgProcessor:
    def __init__(self):
        self.cl_ip, self.cl_port = ('',0)
        self.loc_ip,self.loc_port = ('',0)

    def processIn(self,mesg) :
        return(json.loads(mesg))

    def processOut(self,mesg) :
        return json.dumps(mesg)
           
#==================================================

class SocketClient(Socket) :
    def __init__(self, s=None, port=5005, host = '127.0.0.1', msgSize = 128, ClientMsgProcessor = None):
        super(SocketClient, self).__init__(s,port,host,msgSize)
        if ClientMsgProcessor is not None :
            assert isinstance(ClientMsgProcessor,MsgProcessor), 'ServerMsgProcessor must be a MsgProcessor'
            self.processor = ClientMsgProcessor

    def connect(self):
        try :
            self.s.connect( (self.loc_ip,self.loc_port) )
            (self.cl_ip, self.cl_port) = self.s.getpeername()
            (self.loc_ip,self.loc_port) = self.s.getsockname()
            if self.processor is not None :
                (self.processor.cl_ip,self.processor.cl_port) = (self.cl_ip, self.cl_port)
                (self.processor.loc_ip,self.processor.loc_port) = (self.loc_ip,self.loc_port)
        except socket.error:
            print ("La connexion a échoué (%s)."%(self.loc_ip+':'+str(self.loc_port)))
            sys.exit()
        print ("Connexion établie avec le serveur (%s)."%(self.cl_ip+':'+str(self.cl_port)))

    def sendMsg(self, mesg) :
        self.connect()
        if self.processor is not None:
            msg = self.processor.processOut(mesg)
        else: msg = json.dumps(mesg)
        
        self.putMsg(msg)

        msgServeur = self.getMsg()

        # print("Réponse : %s"%msgServeur)
        if msgServeur == b"0" :
            print("pb d'échange (2)")
            sys.exit()        

        # print ("Connexion terminée.")

        self.close()

        if self.processor is not None:
            res = self.processor.processIn(msgServeur)
        else: res = json.loads(msgServeur)

        return res

    def sendMsg2(self, mesg) :
        self.connect()
        if self.processor is not None:
            msg = self.processor.processOut(mesg)
        else: msg = json.dumps(mesg)
        # print("Envoie au serveur : %s"%msg)
        self.putMsg2(msg)  
        # print ("Connexion terminée.")
        self.close()

#==================================================
#==================================================
#==================================================

if __name__ == "__main__":
    print("main ldqp")


