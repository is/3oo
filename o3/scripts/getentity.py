import os, sys, time
import socket
import random

O3_BASE_PATH = '/is/app/o3'
O3_LIB_PATH = ['base', 'lib/o3']

sys.path.extend([ '%s/%s' % (O3_BASE_PATH, lib) for lib in O3_LIB_PATH])

from o3grid import constants as CC
from o3grid.protocol import O3Channel, O3Call

# -----
def ReadConfigStrFromFile(fn):
	fin = file(fn)
	contents = fin.read()
	fin.close()
	return contents.strip()

# =====
def GetEntity(entity, out):
	# get warehouse service entry
	warehousenode = ReadConfigStrFromFile(O3_BASE_PATH + '/etc/WAREHOUSE')
	print O3_BASE_PATH
	warehouse = (socket.gethostbyname(warehousenode + '-in'), CC.DEFAULT_PORT)

	# get entity id
	res = O3Call(warehouse, 
		CC.SVC_WAREHOUSE, 'LISTENTITY0', entity)
	if res[0] != CC.RET_OK:
		raise 'WAREHOUSE.LISTENTITY0:CALL'
	if len(res[2]) != 1:
		raise 'WAREHOUSE.LISTENTITY0:INVALID-NAME'
	
	entityinfo = res[2][0]
	eid = entityinfo[0]
	esize = entityinfo[3]

	#print 'Eid:%d Esize:%d' % (eid, esize)
	# get shadows'id and location
	res = O3Call(warehouse,
		CC.SVC_WAREHOUSE, 'LISTENTITYLOCATION0', [eid,])
	if res[0] != CC.RET_OK:
		raise 'WAREHOUSE.LISTENTITYLOCATION0:CALL'
	shadows = res[2][eid]
	if len(shadows) < 1:
		raise 'WAREHOUSE.LISTENTITYLOCATION0:NO-SHADOW-COPY'
	
	sid, snode, saddr, slabel, sname, ssize = random.choice(shadows)
	# check out type, create output file object
	if out == None:
		fout = sys.stdout
	if type(out) == str:
		fout = file(out, 'w')
	if type(out) == file:
		fout = out
	else:
		raise 'XX:OUT'
	
	S = O3Channel().connect((saddr, CC.DEFAULT_PORT))
	res = S(CC.SVC_SPACE, 'ROOMGET', slabel, sname, 0, ssize, eid)
	if res[0] == CC.RET_ERROR:
		raise 'SPACE.ROOMGET'
	bs = 512000 * 8
	rest = ssize
	while rest != 0:
		if rest > bs:
			buf = S.recvAll(bs)
		else:
			buf = S.recvAll(rest)
		if not buf:
			break
		rest -= len(buf)
		fout.write(buf)
	
	S.close()
	return ssize

def maindn():
	if sys.argv[2] != '-':
		fout = file(sys.argv[2], 'w')
	else:
		fout.sys = stdout
	try:
		GetEntity(sys.argv[1], fout)
	except:
		sys.exit(1)
	finally:
		fout.close()
	sys.exit(0)

if __name__ == '__main__':
	maindn()
