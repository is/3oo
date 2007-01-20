#!/usr/bin/python

import pprint
from o3grid import constants as CC
from o3grid.protocol import O3Call, O3Channel
import time

res = O3Call(('p-dx44-in', CC.DEFAULT_PORT), CC.SVC_WAREHOUSE, 'FLUSHDB')

pprint.pprint(res)
