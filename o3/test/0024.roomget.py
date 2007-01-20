#!/usr/bin/python

import pprint
import socket
from o3grid import constants as CC
from o3grid.protocol import O3Channel, O3Call, GetMessageFromSocket
import time

#res = O3Call(('p-dx59-in', CC.DEFAULT_PORT),
#	CC.SVC_SPACE, 'ROOMENTITYSPLIT0', '0', 'plog/uume/2006/12/26/2100', 1024 * 1024 * 256)
#pprint.pprint(res)
S = O3Channel().connect(('p-dx63-in', CC.DEFAULT_PORT))
res = S(CC.SVC_SPACE, 'ROOMGET', '0', 'plog/uume/2006/12/26/2100', 0, 1242365418)
pprint.pprint(res)
buf = S.socket.recv(1242365418, socket.MSG_WAITALL)
res = S.getMessage()
pprint.pprint(res)
S.close()


#name = 'plog/uume/2005/12/%02d/%02d00' % (d, h)
#path = '/pub/plog/data/2006/12/%02d/%02d00' % (d, h)
#print name, path
#S.close()
#print name
#pprint.pprint(res)
