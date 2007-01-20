#!/usr/bin/python

import pprint
from o3grid import constants as CC
from o3grid.protocol import O3Channel
import time


S = O3Channel().connect(('localhost', CC.DEFAULT_PORT))
res = S(CC.SVC_WAREHOUSE, 'LISTROOM')
pprint.pprint(res[2])
for r in res[2]:
	res = S(CC.SVC_WAREHOUSE, 'CLEANROOM', r[0])
	pprint.pprint(res)
#res = S(CC.SVC_WAREHOUSE, 'CLEANROOM', 1)
#pprint.pprint(res)

S.close()

#name = 'plog/uume/2005/12/%02d/%02d00' % (d, h)
#path = '/pub/plog/data/2006/12/%02d/%02d00' % (d, h)
#print name, path
#S.close()
#print name
#pprint.pprint(res)
