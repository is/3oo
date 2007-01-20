#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Autoconfig server in O3 grids
#

import threading
import sys

from service import ServiceBase
import constants as CC

class AutoConfigService(ServiceBase):
	SVCID = CC.SVC_AUTOCONFIG
	svcDescription = "Auto config service"
	svcName = 'AUTOCONFIG'
	svcVersion = '0.0.1.0'

	def __init__(self, server):
		self.server = server
	
	def setup(self, cf):
		cf = cf.get('autoconfig')
		self.policyName = cf['policy']
		__import__(self.policyName)
		self.policy = sys.modules[self.policyName].Policy(self)

	def exportAUTOCONFIG0(self, channel, group, hostid):
		return self.policy.autoConfig0(channel, group, hostid)
	
	def exportRELOADPOLICY(self, channel):
		del sys.modules['o3grid.autoconfigpolicy']
		del self.policy
		__import__('o3grid.autoconfigpolicy')
		self.policy = sys.modules['o3grid.autoconfigpolicy'].Policy(self)

		return (CC.RET_OK, self.SVCID, self.policy.getVersion())

