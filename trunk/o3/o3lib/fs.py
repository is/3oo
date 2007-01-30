from struct import pack as ipack, unpack as iunpack
from zlib import decompress as _decompress, MAX_WBITS

from o3grid import constants as CC
from o3grid.protocol import O3Call, O3Channel
# ------
# File services ...
# ------

def O3EntityReader0(queue, **P):
	try:
		node = P['node']
		addr = P['addr']
		label = P['label']
		name = P['name']
		bs = P.get('blocksize', 8388608)
		entityid = P.get('entityid', 0)
		
		size = 0
	
		if name.endswith('.iz0'):
			S = O3Channel().connect((addr, CC.DEFAULT_PORT))
			res = S(CC.SVC_SPACE, 'ROOMGET3',
				{'label':label, 'name':name, 'entityid':entityid})
			if res[0] != CC.RET_OK:
				return
			blocks = res[2]

			for i in xrange(blocks):
				headstr = S.recvAll(32)
				print len(headstr)
				blockhead = iunpack('QII4I', headstr)
				binsize = blockhead[1]
				boutsize = blockhead[2]
				ccontent = S.recvAll(binsize)
				print len(ccontent)
				#content = _decompress(ccontent, -MAX_WBITS, boutsize)
				content = _decompress(ccontent)
				queue.put(content)

			S.getMessage()
			S.close()
		else:
			S = O3Channel().connect((addr, CC.DEFAULT_PORT))
			res = S(CC.SVC_SPACE, 'ROOMGET1', label, name, 0, 0, entityid)
	
			if res[0] != CC.RET_OK:
				return
			size = res[2]

			rest = size
			while rest != 0:
				blocksize = min(rest, bs)
				content = S.recvAll(blocksize)
				rest -= blocksize
				queue.put(content)
			S.getMessage()
			S.close()
	finally:
		queue.put(None)

# ------
O3EntityReader = O3EntityReader0
