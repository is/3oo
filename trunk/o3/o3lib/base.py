#
# O3 base library entry
# 

from o3grid import constants as CC
from o3grid.protocol import O3Call, O3Channel

__VERSION__ = '0.0.0.1'
__REVISION__ = '$REVISION$'


class O3(object):
	def __init__(self, workspace):
		self.ws = workspace
		self.localnames = {}
	
	def saveResult(self, name, value):
		respoint = self.localnames.get(
			'RESULT', self.ws.server.resolv('RESULT'))
		res = O3Call(respoint,
			CC.SVC_SPACE, 'RESULTPUT', name, value)
		if res[0] == CC.RET_OK:
			return res[2]
		else:
			return -1

		
	def loadResult(self, name):
		respoint = self.localnames.get(
			'RESULT', self.ws.server.resolv('RESULT'))
		res = O3Call(respoint,
			CC.SVC_SPACE, 'RESULTGET', name)

		if res[0] != CC.RET_OK:
			return None
		return res[2]
