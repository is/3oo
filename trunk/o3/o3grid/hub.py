#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   HUB Service
#
import os
import sys

import constants as CC
from service import ServiceBase
from protocol import O3Channel, O3Call
from utility import D as _D

class HubService(ServiceBase):
	SVCID = CC.SVC_HUB
	svcDescription = "HUB Service"
	svcName = 'HUB'
	svcVersion = '0.0.1.4'

	def __init__(self, server):
		self.server = server
		self.codebase = {}
	
	def setup(self, config):
		cf = config['hub']
		self.paths = cf['paths']
		sys.path.append(self.paths['codebase'])

		
	def loadCodeBase(self, name):
		path = '%s/%s.codebase' % (self.paths['codebase'], name)
		if not os.path.isfile(path):
			return None

		fin = file(path, 'r')
		content = fin.read()
		fin.close()

		l = {}
		try:
			exec content in globals(), l
		except:
			return None

		if l.has_key('codebase'):
			return l['codebase']
		return None

	def unloadCodeBase(self, name):
		codebase = self.loadCodeBase(name)
		if self.codebase.has_key(name):
			del self.codebase[name]

		for m in codebase['modules']:
			try:
				del sys.modules[m]
			except KeyError:
				pass
		return True

	def cleanCodeBaseCache(self, names):
		ret = list()
		if type(names) == str:
			names = (names, )

		for name in names:
			if self.codebase.has_key(name):
				del self.codebase[name]
				ret.append(name)

		return ret

	# ---
	def _o3unloadCodeBase(self, name, node):
		oc = O3Channel()
		oc.connect(node[1])
		oc(CC.SVC_WORKSPACE, 'UNLOADCODEBASE', name)
		oc.close()


	# ---
	def exportO3UNLOADCODEBASE(self, channel, name):
		# First: Clean codebase in hub scope
		self.unloadCodeBase(name)

		S = O3Channel()
		S.connect(self.server.resolv('SCHEDULE'))
		res = S(CC.SVC_SCHEDULE, 'LISTWORKSPACES')
		nodes = res[2]
		S.close()

		# Three: Clean codebase in workspace scope on all nodes
		for node in nodes:
			self.server.delayCall0(self._o3unloadCodeBase, name, node)

		_D('O3 unload codebase {%s} in %d nodes' % (name, len(nodes)))
		return (CC.RET_OK, self.SVCID, name)

	# ---
	def exportGETCODEBASE(self, channel, name, version):
		if self.codebase.has_key(name):
			return (CC.RET_OK, self.SVCID, self.codebase[name])

		codebase = self.loadCodeBase(name)
		if codebase == None:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

		self.codebase[name] = codebase
		return (CC.RET_OK, self.SVCID, codebase)
	
	# FEATURE/448
	# ---
	def exportUNLOADO3LIB(self, channel):
		res = O3Call(self.server.resolv('SCHEDULE'),
			CC.SVC_SCHEDULE, 'LISTWORKSPACES')

		for node in res[2]:
			O3Call(node[1], CC.SVC_WORKSPACE, 'UNLOADO3LIB')
		return (CC.RET_OK, self.SVCID, len(res[2]))
	# ---
	def exportUNLOADCODEBASE(self, channel, name):
		self.unloadCodeBase(name)
		return (CC.RET_OK, self.SVCID, 0)

	# ---
	def exportCLEANCODEBASECACHE(self, channel, names):
		ret = self.cleanCodeBaseCache(names)
		return (CC.RET_OK, self.SVCID, ret)
	
	def exportLISTCODEBASECACHE(self, channel):
		return (CC.RET_OK, self.SVCID, self.codebase.keys())
		
	# ---
	def exportGETSCRIPTFILE(self, channel, name):
		path = '%s/%s' % (self.paths['scriptbase'], name)
		if not os.path.isfile(path):
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_NO_SUCH_SNIP)

		fin = file(path, 'r')
		contents = fin.read()
		fin.close()

		return (CC.RET_OK, self.SVCID, name, len(contents), contents)
	
	# ---
	def exportNODEJOIN(self, channel, nodeinfo):
		return (CC.RET_OK, self.SVCID, nodeinfo['id'])
	
	def exportNODELEAVE(self, channel, nodeid):
		return (CC.RET_OK, self.SVCID, nodeid)
