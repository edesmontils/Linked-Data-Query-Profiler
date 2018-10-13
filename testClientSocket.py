import socket, sys
import json 
from tools.Socket import SocketClient, MsgProcessor

class MyMsgProcessor(MsgProcessor):
	def __init__(self):
		super(MyMsgProcessor, self).__init__()

	def processOut(self,mesg) :
		rep = json.dumps(mesg)
		print('Client out:',rep)
		return rep

	def processIn(self,mesg) :
		rep = json.loads(mesg)
		print('Client in:',rep)
		return rep

client = SocketClient(ClientMsgProcessor = MyMsgProcessor() )

d = { 'path' : '/run' , 'info' : 'bbbbbbbbbbbbbbbbbbbbbbbbfffffffffffffffffffffffffffffffffffffffffffffffffffffffbbcccccccccccccccccccccccccccccccccccccccccccccc'}

client.sendMsg2(d)


