#!/usr/bin/python

import pprint,sys
import time
from o3grid import constants as CC
from o3grid.protocol import O3Call

#S = O3Channel()
#S.connect(('127.0.0.1', CC.DEFAULT_PORT))
#res = S(CC.SVC_SCHEDULE, 'SUBMITMISSION', 
#	'ls01', {
#		'module': 'logsplit01.logsplit01',
#		'missionclass': 'O3Mission',
#	})

if len(sys.argv) >= 2:
	datename = sys.argv[1]
else:
	datename = '2007/01/18'

dname = datename.replace('/', '.')
res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
	CC.SVC_HUB, 'O3UNLOADCODEBASE', 'oneday01')


for logname in ('uume', 'dzh', 'tt', 'itv'):
	res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
		CC.SVC_SCHEDULE, 'CLEANMISSION', 'OD01-%s-%s' % (logname, dname))

time.sleep(2)
for logname in ('uume', 'dzh', 'tt', 'itv'):
	res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
		CC.SVC_SCHEDULE, 'SUBMITMISSION',
		'OD01-%s-%s' % (logname, dname),
		{
			'module': 'oneday01.oneday01',
			'missionclass': 'O3Mission',
			'prefix': 'plog/%s/%s' % (logname, datename),
		})
