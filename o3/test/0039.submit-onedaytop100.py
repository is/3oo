#!/usr/bin/python

import pprint,sys
import time
from o3grid import constants as CC
from o3grid.protocol import O3Call
import o3testmisc


if len(sys.argv) >= 2:
	prefix = sys.argv[1]
else:
	prefix = 'uume/2007/01/18'


logname, sep, datename = prefix.partition('/')
mid = 'ODT1-%s-%s' % (logname, datename.replace('/', '.'))
prefix = 'plog/' + prefix


if o3testmisc.IsDebugMission('onedaytop100'):
	res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
		CC.SVC_SCHEDULE, 'CLEANMISSION', mid)
	res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
		CC.SVC_HUB, 'O3UNLOADCODEBASE', 'onedaytop100')
	time.sleep(2)

res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
	CC.SVC_SCHEDULE, 'SUBMITMISSION',
	mid, {
		'module': 'onedaytop100.onedaytop100',
		'missionclass': 'O3Mission',
		'prefix': prefix,
	})
