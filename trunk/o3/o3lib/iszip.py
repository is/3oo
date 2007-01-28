# 
# Special compress file format for O3 warehouse
#

# File Structure
# Offset  Length 
#      0      4B  "ISZ0" (4char)
#             4B  FLAGS (dowrd)
#             4B  VERSION (dword)
#             4B  NOUSED, 0
#     16      4B  "HD01" (4char)
#             4B  NOUSED, 0
#             4B  FILE BLOCKS
#             4B  ONE BLOCK UNCOMPRESS SIZE
#             8B  FILE COMPRESSED SIZE
#             8B  FILE DECOMPRESSED SIZE
#     48     16B  NOUSED, 0
#            32B  BLOCK_ENTRY
#                 .......
#  65536          BLOCK
#
# ------
# Block entry structure:
#      0      8B  OFFSET
#      8      4B  BLOCK SIZE
#     12      4B  UNCOMPRESSED SIZE
#     16     16B  NOUSED - available for other used

# ------

import os, sys, zlib
import binascii
from zlib import compress as _compress, decompress as _decompress
import struct

#class Zipis(object):
#	def __init__(self, name): pass

def CompressFile(finame, foname, linemode = True, bs = 16777216, level = 6):
	fin = file(finame, 'rb')
	fout = file(foname, 'wb')

	bi = list() # block index
	dbb = 0 # data block base
	idsize = 0 # input data size
	odsize = 0 # output data size

	# seek fout to data block
	fout.seek(0x10000, 0)
	print "%X" % fout.tell()

	looping = True

	while looping:
		content = fin.read(bs)
		if not content: # true if reach end of file
			looping = False
			break
		else:
			if linemode: # check end of line is end of block
				if content[-1] != '\n':
					offset = content.rfind('\n')
					if offset != -1:
						clen = len(content)
						content = content[:offset + 1]
						fin.seek(len(content) - clen, 1)

		ccontent = _compress(content)
		fout.write(ccontent)
		bi.append((odsize, len(ccontent), len(content)))
		print '%d - %d %d %d %s' % (len(bi), odsize, len(ccontent), len(content), binascii.b2a_hex(ccontent[:16]))
		odsize += len(ccontent)
		idsize += len(content)
	
	# data compressing finished, build header and write to fout's begin.
	head0 = struct.pack(
		'4sIII4sIIIQQ4I',
		'ISZ0', 0, 0, 0, 
		'HD01', 0, len(bi), bs,
		odsize, idsize,
		0, 0, 0, 0)
		
	head1 = ''.join([
		struct.pack("QII4I", x[0], x[1], x[2], 0, 0, 0, 0) for x in bi
		])

	fout.seek(0)
	fout.write(head0)
	fout.write(head1)

	fin.close()
	fout.close()
	
def DecompressFile(finame, foname):
	fin = file(finame, 'rb')
	fout = file(foname, 'wb')

	head = fin.read(0x10000)
	filehead = struct.unpack("4sIII4sIIIQQ4I", head[:64])
	blocks = filehead[6]
	blocksize = filehead[7]

	for i in xrange(blocks):
		blockhead = struct.unpack("QII4I", head[64 + i * 32: 64 + i * 32 + 32])
		print "%d - %d,%d,%d" % (i, blockhead[0], blockhead[1], blockhead[2])
		binsize = blockhead[1]
		boutsize = blockhead[2]

		ccontent = fin.read(binsize)
		print binascii.b2a_hex(ccontent[:16])
		content = _decompress(ccontent)
		fout.write(content)
	fin.close()
	fout.close()

if __name__ == '__main__':
	CompressFile('/tmp/2300', '/tmp/2300.iz')
	DecompressFile('/tmp/2300.iz', '/tmp/2300_')
