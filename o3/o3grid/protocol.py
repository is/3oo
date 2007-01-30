#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Protocol
#

#from cPickle import loads, dumps, HIGHEST_PROTOCOL
from fastmap import _loads as loads, _dumps as dumps
from struct import pack, unpack
from zlib import compress as _compress, decompress as _dcompress

import socket

from o3grid import constants as CC
	
def CreateMessage(*ins):
	#buf = dumps(ins, HIGHEST_PROTOCOL)
	buf =dumps(ins)
	buflen = pack('!I', len(buf))
	return ''.join((buflen, buf))

def CreateMessage0(ins):
	#buf = dumps(ins, HIGHEST_PROTOCOL)
	buf = dumps(ins)
	buflen = pack('!I', len(buf))
	return ''.join((buflen, buf))

def GetMessageFromSocket(ins):
	head = ins.recv(4)
	buflen = unpack('!I', head)[0]

	got = 0
	contents = []

	while got != buflen:
		buf = ins.recv(buflen - got)
		got += len(buf)
		contents.append(buf)
	
	return loads(''.join(contents))

def GetDataFromSocketToFile(sin, fout, size):
	rest = size
	flags = socket.MSG_WAITALL
	while rest != 0:
		if rest > 512000:
			blocksize = 512000
		else:
			blocksize = rest
		contents = sin.recv(blocksize, flags)
		
		if not contents:
			return size - rest

		fout.write(contents)
		rest -= len(contents)
	
	return size

def GetDataFromSocketToISZIP(
	sin, foname, size, linemode = True, bs = 16777216, level = 6):

	rest = size
	waitall = socket.MSG_WAITALL

	bi = []
	fout = file(foname, 'wb')
	fout.write(chr(0) * 0x10000)
	odsize = 0
	idsize = 0
	pending = ''

	while True:
		blocksize = min(rest, bs)
		if blocksize == 0:
			if not pending:
				break
			content = pending
		else:
			content = sin.recv(blocksize, waitall)
			rest -= len(content)
			if linemode:
				if content[-1] != '\n':
					o = content.rfind('\n')
					if o != -1:
						newpending = content[o + 1:]
						content = content[:o + 1]
					else:
						newpending = ''

					if pending:
						content = pending + content
					pending = newpending
				else:
					if pending:
						content = pending + content
						pending = ''
		ccontent = _compress(content, level)
		bi.append((odsize, len(ccontent), len(content)))
		odsize += len(ccontent)
		idsize += len(content)
		fout.write(ccontent)

	head0 = pack(
		'4sIII4sIIIQQ4I',
		'ISZ0', 0, 0, 0,
		'HD01', 0, len(bi), bs,
		odsize, idsize, 
		0, 0, 0, 0)
	head1 = ''.join([
		pack('QII4I', x[0], x[1], x[2], 0, 0, 0, 0) for x in bi
	])

	fout.seek(0)
	fout.write(head0)
	fout.write(head1)
	fout.close()

	return odsize

# ======
class O3Channel(object):
	def __init__(self):
		self.socket = None
		pass
	
	def connect(self, addr):
		self.addr = addr 
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
		self.socket.connect(self.addr)
		return self
	
	def __call__(self, *params):
		self.socket.send(CreateMessage0(params))
		return GetMessageFromSocket(self.socket)
	
	def getMessage(self):
		return GetMessageFromSocket(self.socket)

	def close(self):
		if self.socket:
			self.socket.close()
			self.socket = None

	def recvAll(self, len):
		return self.socket.recv(len, socket.MSG_WAITALL)
	
	def sendAll(self, buffer):
		return self.socket.sendall(buffer)
	
# ======
def O3Call(entry, *param):
	S = O3Channel().connect(entry)
	res = S(*param)
	S.close()
	return res

# ===
class O3Space(object):
	def __init__(self, addr = None):
		self.addr = addr
		self.error = 0
	
	def PUT(self, id, content):
		S = O3Channel()
		try:
			length = len(content)
			if self.addr:
				S.connect(self.addr)
			else:
				S.connect(('127.0.0.1', CC.DEFAULT_PORT))
			res = S(CC.SVC_SPACE, 'PUT', id, length, None)
			if res[0] != CC.RET_CONTINUE:
				self.error = res[2]
				return False

			S.sendAll(content)
			res = S.getMessage()
			if res[0] == CC.RET_OK:
				return True
			self.error = res[2]
			return False
		finally:
			S.close()
	
	def GET(self, id):
		S = O3Channel()
		try:
			S.connect(self.addr)
			res = S(CC.SVC_SPACE, 'GET', id, None)
			if res[0] == CC.RET_ERROR:
				self.error = res[2]
				return None

			length = res[3]
			content = S.recvAll(length)

			if len(content) != length:
				return None
			res = S.getMessage()
			return content
		finally:
			S.close()
