#!/usr/bin/python

import pprint,sys
from o3grid import constants as CC
from o3grid.protocol import O3Call

res = O3Call(('127.0.0.1', CC.DEFAULT_PORT),
	CC.SVC_HUB, 'UNLOADO3LIB')
