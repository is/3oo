#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Base service module
#

import threading
import socket
import cPickle as pickle
import struct
import os

import constants as CC
from protocol import CreateMessage0, GetMessageFromSocket, CreateMessage


class ServiceException(Exception):
	def __init__(self, *param):
		Exception.__init__(self, *param)
		
# ====
class ServiceBase(object):
	# ----
	def dispatch(self, channel, param):
		funcname = param[1]
		try:
			func = getattr(self, 'export%s' % funcname)
		except AttributeError:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_FUNCTION)
		param = param[2:]
		return func(channel, *param)

	# ----
	def setup(self, conf): pass
	def activate(self): pass

	# ----
	def getCurrentPingInfo(self):
		return 'OK'

# ====
class BaseService(ServiceBase):
	SVCID = CC.SVC_BASE
	svcName = 'BASE'
	svcVersion = '0.0.0.1'
	svcDescription = "Base Service"

	def __init__(self, server):
		self.server = server 
	
	def exportLISTSERVICE(self, channel):
		return (CC.RET_OK, self.SVCID, self.root.svc.keys())

	def exportSHELLSCRIPT(self, channel, script):
		fin = os.popen(script)
		content = fin.read()
		fin.close()
		return (CC.RET_OK, self.SVCID, content)
		
	def exportPYTHONSCRIPT(self, channel, script):
		try:
			g = globals()
			l = {}
			exec script in g, l
			return (CC.RET_OK, self.SVCID, l.get('result', None))

		except:
			return (CC.RET_ERROR, self.SVCID, 0)

# ====
class EchoService(ServiceBase):
	SVCID = CC.SVC_ECHO
	svcDescription = "Echo Service"

	def exportECHO(self, channel, str):
		return (CC.RET_OK, self.SVCID, str)
