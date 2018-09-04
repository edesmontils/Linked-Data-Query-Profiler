import socket, sys
import json 
from tools.Socket import SocketServer, MsgProcessor


class MyMsgProcessor(MsgProcessor):
	def __init__(self):
		super(MyMsgProcessor, self).__init__()

	def processIn(self,mesg) :
		rep = json.loads(mesg)
		print(rep)
		return json.dumps(  [0,0,0,0]  )

server = SocketServer(ServerMsgProcessor = MyMsgProcessor() )
server.run2()

