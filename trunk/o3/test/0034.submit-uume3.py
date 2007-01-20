#!/usr/bin/python

import pprint, sys, time
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
	prefix = sys.argv[1]
else:
	prefix = 'plog/uume/2006/12/31'

res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
	CC.SVC_HUB, 'O3UNLOADCODEBASE', 'uume03')

time.sleep(2)

res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
	CC.SVC_SCHEDULE, 'SUBMITMISSION',
	'uume03', {
		'module': 'uume03.uume03',
		'missionclass': 'O3Mission',
		'prefix': prefix,
	})
