#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Name server in O3 grids
#

import threading

from service import ServiceBase
import constants as CC

class NameService(ServiceBase):
	SVCID = CC.SVC_NAMES
	svcDescription = "Name service"
	svcName = 'NAMES'
	svcVersion = '0.0.1.0'

	def __init__(self, server):
		self.server = server
		self.lock = threading.Lock()
		self.names = {}
	
	def setup(self, conf):
		cf = conf.get('names', None)
		if not cf:
			return

		if cf.has_key('names'):
			self.names.update(cf['names'])

	def exportRESOLV(self, channel, name):
		return (CC.RET_OK, self.SVCID, self.names.get(name))
			
	def exportADD(self, channel, name, value, override = False):
		self.lock.acquire()
		try:
			old = self.names.get(name, None)
			if old:
				if not override:
					return (CC.RET_OK, self.SVCID, CC.NAMES_DUP)
				else:
					self.names[name] = value
					return (CC.RET_OK, self.SVCID, CC.NAMES_UPDATE)
			else:
				self.names[name] = value
				return (CC.RET_OK, self.SVCID, CC.NAMES_ADD)
		finally:
			self.lock.release()

	def exportUPDATE(self, channel, name, value):
		self.lock.acquire()
		try:
			if not self.names.has_key(name):
				return (CC.RET_OK, self.SVCID, CC.NAMES_EMPTY)
			self.names[name] = value
			return (CC.RET_OK, self.SVCID, CC.NAMES_UPDATE)
		finally:
			self.lock.release()

	def exportDEL(self, channel, name):
		self.lock.acquire()
		try:
			if not self.names.has_key(name):
				return (CC.RET_OK, self.SVCID, CC.NAMES_EMPTY)
			del self.names[name]
			return (CC.RET_OK, self.SVCID, CC.NAMES_DELETE)
		finally:
			self.lock.release()

	exportDELETE = exportDEL
	def exportGETALL(self, channel):
		self.lock.acquire()
		try:
			return (CC.RET_OK, self.SVCID, self.names.keys())
		finally:
			self.lock.release()
