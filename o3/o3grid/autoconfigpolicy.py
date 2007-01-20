#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Auto Configure Policy
#

import socket
import constants as CC

class AutoConfigPolicy(object):
	def __init__(self, service):
		self.service = service
		self.storageGroup = ['p-cn39', 'p-cn26', 'p-cn53']

	def autoConfig0(self, channel, group, hostid):
		if not hostid.startswith('p-'):
			hid = "p-%s" % hostid
		else:
			hid = hostid

		if hid.startswith('p-cnn'):
			realname = hid
		else:
			realname = '%s-in' % hid

		ip = socket.gethostbyname(realname)
		BASE = '/is/app/o3'

		common = {
			'name': hid[2:],
			'id': hid,
			'entry': (ip, CC.DEFAULT_PORT),
			'zone': 'o3dev',
			'base': BASE,
			'names': {
				'HUB': ('10.6.32.197', CC.DEFAULT_PORT),
				'NAMES': ('10.6.32.197', CC.DEFAULT_PORT),
				'SCHEDULE': ('10.6.32.197', CC.DEFAULT_PORT),
				'WAREHOUSE': ('10.6.32.197', CC.DEFAULT_PORT),
				'RESULT': ('10.4.170.220', CC.DEFAULT_PORT), # p-cn39
			},
			'debug': 'call',
			'ulog': {
				'addr': ('10.6.32.197', CC.DEFAULT_LOG_PORT)
			},
		}

		space = {
			'path': '/'.join((BASE, 'tmp/storage')),
			'roommode': 'autoconfig',
		}

		workspace =  {
			'base': '/'.join((BASE, 'tmp/run')),
		}

		if hid in self.storageGroup:
			workspace['tag'] = 'storage'

		_C = {
			'common': common,
			'space': space,
			'workspace': workspace,
		}

		return (CC.RET_OK, self.service.SVCID, _C)

	def getVersion(self):
		return 'is-autoconfig-0.0.0.2'

Policy = AutoConfigPolicy
