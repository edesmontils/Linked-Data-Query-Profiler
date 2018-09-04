import asyncio
import websockets
import json 

class Run:
	def __init__(self):
		self.rep = None
		asyncio.get_event_loop().run_until_complete(self.hello())

	async def hello(self):
		async with websockets.connect('ws://localhost:5005/run') as websocket:
			name = [1, 2, 3]

			await websocket.send(json.dumps(name))
			print(f"> {name}")
			greeting = await websocket.recv()
			print(f"< {greeting}")
			self.rep = greeting

r = Run()
print(r.rep)
