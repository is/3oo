#!/usr/bin/python

import pprint
import sys

from o3grid import constants as CC
from o3grid.protocol import O3Channel
import time

entitys = ['1']

if len(sys.argv) > 1:
	entitys = []
	for x in sys.argv[1:]:
		try:
			entitys.append(int(x))
		except ValueError:
			entitys.append(x)

S = O3Channel().connect(('localhost', CC.DEFAULT_PORT))
for e in entitys:
	res = S(CC.SVC_WAREHOUSE, 'DROPENTITY', e)
	print res
#res = S(CC.SVC_WAREHOUSE, 'CLEANROOM', 1)
#pprint.pprint(res)

S.close()

#name = 'plog/uume/2005/12/%02d/%02d00' % (d, h)
#path = '/pub/plog/data/2006/12/%02d/%02d00' % (d, h)
#print name, path
#S.close()
#print name
#pprint.pprint(res)
