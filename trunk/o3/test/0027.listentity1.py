#!/usr/bin/python

import pprint
import sys, os
from o3grid import constants as CC
from o3grid.protocol import O3Call, O3Channel
import time

if len(sys.argv) >= 2:
	name = sys.argv[1]
else:
	name = 'plog/uume/2006/12/31/'

res = O3Call(('p-dx44-in', CC.DEFAULT_PORT), CC.SVC_WAREHOUSE, 'LISTENTITY0', name)
pprint.pprint(res)
res = O3Call(('p-dx44-in', CC.DEFAULT_PORT),
	CC.SVC_WAREHOUSE, 'LISTENTITYLOCATION0', [r[0] for r in res[2]])
pprint.pprint(res)
