#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   SNIP
# 
# ==Description
#   Simple space stroage client
#

import socket
import constants as CC
import cStringIO as StringIO
from protocol import CreateMessage, GetMessageFromSocket, O3Space, O3Call

class RemoteSnipClient(object):
	def __init__(self, space):
		self.addr = space
		self.error = None
	
	def getTransport(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
		return s

	def PUT(self, id, content):
		length = len(content)

		s = self.getTransport()
		try:
			s.connect(self.addr)
			s.send(CreateMessage(CC.SVC_SPACE, 'PUT', id, length, None))
			params = GetMessageFromSocket(s)
			if params[0] != CC.RET_CONTINUE:
				self.error = params[2]
				return False
			s.send(content)
			params = GetMessageFromSocket(s)
			if params[0] == CC.RET_OK:
				return True
			else:
				self.error = params[2]
				return False
		finally:
			s.close()

	def GET(self, id):
		s = self.getTransport()
		try:
			s.connect(self.addr)
			s.send(CreateMessage(CC.SVC_SPACE, 'GET', id, None))
			params = GetMessageFromSocket(s)
			if params[0] == CC.RET_ERROR:
				self.error = params[2]
				return None

			length = params[3]
			rest = length
			content = []
			flags = socket.MSG_WAITALL
			while rest != 0:
				if rest > 32768:
					buffer = s.recv(32768, flags)
				else:
					buffer = s.recv(rest)
				if not buffer:
					break
				rest -= len(buffer)
				content.append(buffer)

			if rest != 0:
				self.error = CC.ERROR_NETWORK
				return None
			params = GetMessageFromSocket(s)
			return ''.join(content)
		finally:
			s.close()

	def DELETE(self, id):
		s = self.getTransport()
		try:
			s.connect(self.addr)
			s.send(CreateMessage(CC.SVC_SPACE, "DELETE", id))
			params = GetMessageFromSocket(s)
			if params[0] == CC.RET_OK:
				return True
			else:
				self.error = params[2]
				return False
		finally:
			s.close()
