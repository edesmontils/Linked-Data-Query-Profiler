import socket, sys
import json 
from tools.Socket import SocketClient, MsgProcessor

class MyMsgProcessor(MsgProcessor):
	def __init__(self):
		super(MyMsgProcessor, self).__init__()

	def processOut(self,mesg) :
		return json.dumps(mesg)

	def processIn(self,mesg) :
		return json.loads(mesg)

client = SocketClient(ClientMsgProcessor = MyMsgProcessor() )

d = { 'path' : '/run' , 'info' : 'bbbbbbbbbbbbbbbbbbbbbbbbfffffffffffffffffffffffffffffffffffffffffffffffffffffffbbcccccccccccccccccccccccccccccccccccccccccccccc'}

client.sendMsg2(d)


