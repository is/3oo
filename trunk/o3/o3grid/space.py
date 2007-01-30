#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Space - Simple file storage service
#

from __future__ import with_statement

SPACE_VERSION = '0.0.3.5'

import time, os, threading
import zlib
from struct import pack as ipack, unpack as iunpack
import constants as CC

from service import ServiceBase
from protocol import CreateMessage, O3Channel, O3Call
from protocol import GetDataFromSocketToFile, GetMessageFromSocket
from protocol import GetDataFromSocketToISZIP

from utility import mkdir_p, PrepareDir
from utility import D as _D, D2 as _D2
from utility import RoomLocation, RoomLocationToTuple
from utility import LogFileSpliter


def FileLength(filename):
	s = os.stat(filename)
	return s[6]

# ====
__all__ = (
	'SpaceStroage', 'SpaceItem',
	'FileItem', 'FileStorage',
	'SpaceService',
)

class SpaceItem(object): pass
class SpaceStorage(object): pass

# ===
class FileStorage(SpaceStorage):
	def __init__(self, base):
		self.base = base

class FileItem(SpaceItem):
	BLOCKSIZE = 524288

	def __init__(self, storage, name, size, attrs):
		self.base = storage.base
		self.ready = False
		self.attrs = attrs
		self.name = name
		self.size = size
		self.path = '%s/%s' % (self.base, self.name)
		self.offset = 0
		self.deletable = True

	def isReady(self):
		return self.ready
	
	def fillSnipFromSocket(self, socket):
		fin = file(self.path, 'w')
		try:
			rest = self.size
			while True:
				if rest > self.BLOCKSIZE:
					blocksize = self.BLOCKSIZE
				else:
					blocksize = rest

				buffer = socket.recv(blocksize)
				if not buffer:
					return False
				fin.write(buffer)
				rest -= len(buffer)

				if rest == 0:
					break
		finally:
			fin.close()
		if rest == 0:
			self.ready = True
			return True
		else:
			return False

	def pushSnipToSocket(self, socket):
		fin = file(self.path, 'r')
		if self.offset:
			fin.seek(self.offset)
		try:
			rest = self.size
			while True:
				if rest > self.BLOCKSIZE:
					blocksize = self.BLOCKSIZE
				else:
					blocksize = rest

				buffer = fin.read(blocksize)
				if not buffer:
					return False
				socket.sendall(buffer)
				rest -= len(buffer)

				if rest == 0:
					break
		finally:
			fin.close()
		if rest == 0:
			return True
		else:
			return False


	def unlink(self):
		if self.deletable:
			try:
				os.unlink(self.path)
			except:
				pass
		self.deletable = False
			

# ====
class Room(object):
	def __init__(self, id, label, base, capacity, used):
		self.id = id
		self.label = label
		self.base = base
		self.capacity = capacity
		self.used = used

# ====
class SpaceService(ServiceBase):
	SVCID = CC.SVC_SPACE
	svcDescription = "Data space service (simple filesystem service)"
	svcName = 'SPACE'
	svcVersion = SPACE_VERSION
	ADVERT_INTERVAL = 7

	def __init__(self, server):
		ServiceBase.__init__(self)
		self.lock = threading.Lock()
		self.server = server
		self.snips = {}
		self.rooms = None
		self.roomtasks = {}
	
	# ---
	def setup(self, conf):
		cf = conf['space']
		self.storage = FileStorage(cf['path'])
		self.resultPath = cf.get('respath', '/pub/o3res')

		if cf.get('roommode', None) == 'autoconfig':
			S = O3Channel().connect(self.server.resolv('WAREHOUSE'))
			res = S(CC.SVC_WAREHOUSE, 'AUTOCONFIG', self.server.zone, self.server.id)
			S.close()

			if res[0] == CC.RET_OK and res[2] != None:
				self.setupRoom(res[2])
	
	def activate(self):
		self.server.addTimer2('room_advert', self.ADVERT_INTERVAL, True,
			self.advert, args = ())

	def setupRoom(self, conf):
		self.rooms = {}
		for r in conf['rooms']:
			room = Room(*r)
			self.rooms[room.label] = room
			mkdir_p(room.base)

		# ---
		self.rooms['Z'] = Room(0, 'Z', '/', 0, 0) 
					
	# ---
	def advert(self):
		entry = None
		with self.lock:
			if self.rooms:
				entry = self.server.resolv('WAREHOUSE')
				nodeid = self.server.id
				starttime = self.server.starttime
				tasks = self.roomtasks.keys()
				rooms = [ r.id for r in self.rooms.values() if r.id != 0 ]
				rooms.sort()

		if entry:
			S = O3Channel().connect(entry)
			res = S(CC.SVC_WAREHOUSE, 'ROOMADVERT', 
				nodeid, self.server.entry, starttime, tasks, rooms)
			S.close()

	# ---
	def put2(self, id, path, size, offset,attrs = None, deletable = False):
		if self.snips.has_key(id):
			self.snips[id].unlink()
			del self.snips[id]
		item = FileItem(self.storage, id, size, attrs)
		item.offset = offset
		item.path = path
		item.ready = True
		item.deletable = deletable
		self.snips[id] = item

	# ---
	def exportPUT2(self, channel, id, path, size, 
		offset, attrs, deletable = False):
		self.put2(id, path, size, offset, attrs, deletable)
		return (CC.RET_OK, self.SVCID, id, size)

	def exportPUT(self, channel, id, size, attrs):
		if self.snips.has_key(id):
			self.snips[id].unlink()
			del self.snips[id]

		channel.send(CreateMessage(
			CC.RET_CONTINUE, self.SVCID, 0))

		item = FileItem(self.storage, id, size, attrs)
		item.fillSnipFromSocket(channel)

		if item.isReady():
			self.snips[id] = item
			return (CC.RET_OK, self.SVCID, id, size)
		else:
			del item
			# TODO more error detail here
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_PUT)

	def exportGET(self, channel, id, attrs):
		if not self.snips.has_key(id):
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

		item = self.snips[id]

		channel.send(CreateMessage(CC.RET_CONTINUE, self.SVCID, id, item.size))
		item.pushSnipToSocket(channel)
		return ((CC.RET_OK, self.SVCID, 0),
			'id:%s size:%.2fm' % (id, item.size/1024.0/1024))

	def exportDELETE(self, channel, id):
		if not self.snips.has_key(id):
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_NO_SUCH_SNIP)
		item = self.snips[id]
		item.unlink()
		del self.snips[id]
		return (CC.RET_OK, self.SVCID, id)

	def exportCLEANALL(self, channel):
		for s in self.snips.values():
			s.delete()
		self.snips.clear()
		return (CC.RET_OK, self.SVCID, 0)

	def exportLISTALL(self, channel):
		return (CC.RET_OK, self.SVCID, self.snips.keys())

	def exportLOCALPATH(self, channel, id):
		return (CC.RET_OK, self.SVCID, '%s/%s' % (self.storage.base, id))
	
	# ---
	def exportROOMADDCONFIG(self, channel, ri):
		if self.rooms.has_key(ri[1]):
			return (CC.RET_ERROR, self.SVCID, 
				CC.ERROR_WAREHOUSE_DUPLICATION_ROOMLABEL)
		room = Room(*ri)
		self.rooms[room.label] = room
		mkdir_p(room.base)
		return (CC.RET_OK, self.SVCID, 0)

	
	# ---
	def exportROOMGET1(self, channel, label, path, offset, size, entityid = 0):
		room = self.rooms.get(label, None)
		if room == None:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_NO_SUCH_ROOM)

		path = '%s/%s' % (room.base, path)
		if not os.path.isfile(path):
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

		if size == 0:
			size = FileLength(path) - offset

		channel.send(CreateMessage(CC.RET_OK, self.SVCID, size))
		
		fin = file(path, 'r')
		starttime = time.time()
		if offset:
			fin.seek(offset)

		try:
			rest = size
			while rest != 0:
				if rest > 512000:
					blocksize = 512000
				else:
					blocksize = rest
				contents = fin.read(blocksize)
				channel.sendall(contents)
				rest -= blocksize
		finally:
			fin.close()
		endtime = time.time()

		return (
			(CC.RET_OK, self.SVCID, size), 
			'E-%d -%d %.2fMB/%.2fs' % (
				entityid, offset, size / 1024.0/1024, endtime - starttime))

	# ---

	def exportROOMGET3(self, channel, P):
		label = P['label']
		name = P['name']
		offset = P.get('offset', 0)
		wantblocks = P.get('blocks', 0)
		entityid = P.get('entityid', 0)

		room = self.rooms.get(label, None)
		if room == None:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_NO_SUCH_ROOM)

		path = '%s/%s' % (room.base, name)
		
		if not os.path.isfile(path):
			return (CC.RET_ERROR, self.SVCID, CC_ERROR_NO_SUCH_OBJECT)

		starttime = time.time()
		size0 = 0
		size1 = 0
		try:
			fin = file(path, 'rb')
			headblock = fin.read(0x10000)
			filehead = iunpack('4sIII4sIIIQQ4I', headblock[:64])
			blocks = filehead[6]
			blocksize = filehead[7]
	
			if wantblocks == 0:
				wantblocks = blocks - offset
			else:
				wantblocks = min(blocks - offset, wantblocks)
	
			channel.send(CreateMessage(CC.RET_OK,self.SVCID, wantblocks))
	
			for i in xrange(offset, offset + wantblocks):
				blockheadstr = headblock[64 + i * 32: 64 + i * 32 + 32]
				blockhead = iunpack("QII4I", blockheadstr)
				if i == offset:
					fin.seek(blockhead[0] + 0x10000)
				binsize = blockhead[1]
				size0 += binsize
				size1 += blockhead[2] # boutsize
	
				ccontent = fin.read(binsize)
				channel.sendall(blockheadstr)
				channel.sendall(ccontent)
			endtime = time.time()
			return ((CC.RET_OK, self.SVCID, wantblocks),
				'E-%d -%d %.2fMB(%.2fMB)/%.2fs' % (
					entityid, offset, size1/1024.0/1024, size0/1024.0/1024,
					endtime - starttime))

		finally:
			fin.close()

	# ---
	def exportROOMDROPSHADOW(self, channel, label, name):
		room = self.rooms.get(label)
		path = '%s/%s' % (room.base, name)

		try:
			os.unlink(path)
			_D2('ROOM.DROPSHADOW %s' % path)
		except OSError, e:
			return (CC.RET_ERRPR, self.SVCID, e.errno)
		return (CC.RET_OK, self.SVCID, 0)
			
	def exportROOMSHADOWLIST(self, channel, tag):
		if type(tag) == str:
			room = self.rooms.get(tag, None)
		else:
			room = None

		if not room:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

		files = []
		for path, dirnames, filenames in os.walk(room.base):
			files.extend(['%s/%s' % (path, f) for f in filenames])

		baselen = len(room.base) + 1
		return ((CC.RET_OK, self.SVCID, [ x[baselen:] for x in files ]), 
			{'shadows': len(files),})

	# ---
	def exportROOMCLEAN(self, channel, label, names):
		count = 0
		room = self.rooms.get(label)

		files = []
		dirs = []
		base = room.base

		for path, dirnames, filenames in os.walk(room.base):
			files.extend([ '%s/%s' % (path, f) for f in filenames])
			dirs.extend([ '%s/%s' % (path, d) for d in dirnames])

		files2 = set([ '%s/%s' % (base, name) for name in names])

		for  path in files:
			if path not in files2:
				_D2('ROOM.CLEAN remove %s' % (path))
				os.unlink(path)
				count += 1

		dirs.sort()
		dirs.reverse()

		for d in dirs:
			try:
				os.rmdir(d)
				_D2('ROOM.CLEAN rmdir %s' %  d)
				count += 1
			except OSError, e:
				pass
		return (CC.RET_OK, self.SVCID, count)

	# ---
	def exportROOMRECONFIG(self, channel):
		S = O3Channel().connect(self.server.resolv('WAREHOUSE'))
		res = S(CC.SVC_WAREHOUSE,
			'AUTOCONFIG', self.server.zone,
			self.server.id)
		S.close()
		if res[0] == CC.RET_OK and res[2] != None:
			self.setupRoom(res[2])
		else:
			self.rooms = None

		return (CC.RET_OK, self.SVCID, res[0])
		
	# ---
	def exportADDENTITY(self, channel, name, path):
		if name.startswith('/'):
			name = name.strip('/')

		try:
			s = os.stat(path)
		except OSError:
			return (CC.RET_OK, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

    # Perform a stat() system call on the given path. The return 
		# value is an object whose attributes correspond to the members 
		# of the stat structure, namely: 
		# st_mode (protection bits), st_ino (inode number), st_dev (device), 
		# st_nlink (number of hard links), st_uid (user ID of owner), 
		# st_gid (group ID of owner), st_size (size of file, in bytes), 
		# st_atime (time of most recent access), 
		# st_mtime (time of most recent content modification), 
		# st_ctime (platform dependent; 
		# time of most recent metadata change on Unix, or the time 
		# of creation on Windows).
		ei = {
			'name': name,
			'node': self.server.id,
			'entry': self.server.entry,
			'path': path.strip('/'),
			'size': s[6],
			'mtime': s[8],
		}

		S = O3Channel().connect(self.server.resolv('WAREHOUSE'))
		res = S(CC.SVC_WAREHOUSE, 'ADDENTITY', ei)
		S.close()
		return res

	# ---
	def exportROOMMIRROR(self, channel, task):
		thr = threading.Thread(
			name = task['taskid'],
			target = self.mirrorRoom,
			args = (task,))
		thr.setDaemon(1)
		thr.start()
		return (CC.RET_OK, self.SVCID, 0)

		
		
	def mirrorRoom(self, task):
		task['result'] = 0
		whentry = self.server.resolv('WAREHOUSE')

		S = O3Channel()
		with self.lock:
			self.roomtasks[task['taskid']] = task
		try:
			srcloc = task['source']
			snode, sentry, slabel, spath = RoomLocationToTuple(srcloc)
			droom = self.rooms[task['destroomlabel']]
			#size = task['size']
			size = 0
			localpath = '%s/%s' % (
				droom.base, task['name'])

			# open remote file
			S.connect(sentry)
			res = S(CC.SVC_SPACE, 'ROOMGET1', slabel, spath, 0, size, task['entityid'])

			if res[0] == CC.RET_OK:
				size = res[2]
				mkdir_p(os.path.dirname(localpath))
				if localpath.endswith('.iz0') and not spath.endswith('.iz0'):
					odsize = GetDataFromSocketToISZIP(S.socket, localpath, size)
					task['compress'] = 'iz0'
					task['compressedsize'] = odsize
				else:
					fout = file(localpath, 'w')
					GetDataFromSocketToFile(S.socket, fout, size)
				res = GetMessageFromSocket(S.socket)
			else:
				task['result'] = res[2]

		finally:
			S.close()

			try:
				O3Call(whentry, CC.SVC_WAREHOUSE, 'MIRRORFINISHED', task)
			except: pass
			with self.lock:
				del self.roomtasks[task['taskid']]
	
	# ---
	def exportROOMENTITYSPLIT0(self, channel, label, name, bs, etc = 1.1):
		room = self.rooms.get(label, None)
		if room == None:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_SPACE_NO_SUCH_ROOM)

		fullpath = '/'.join((room.base, name))
		if not os.path.isfile(fullpath):
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

		spliter = LogFileSpliter(fullpath, bs, etc)
		spliter.splitAtLineEnd()
		return (CC.RET_OK, self.SVCID,
			spliter.size, spliter.res)
		
	# ---
	def exportRESULTPUT(self, channel, name, value):
		fname = '/'.join((self.resultPath, name))
		PrepareDir(fname)
		fout = file(fname, 'w')
		fout.write(value)
		fout.close()

		return (
			(CC.RET_OK, self.SVCID, len(value)),
			"%s %d" % (name, len(value)))

	# ---
	def exportRESULTGET(self, channel, name):
		fname = '/'.join((self.resultPath, name))

		try:
			fin = file(fname)
			value = fin.read()
			fin.close()

			return (
				(CC.RET_OK, self.SVCID, value),
				"%s %d" % (name, len(valne)))
		except IOError, e:
			return (
				(CC.RET_ERROR, self.SVCID, e.errno, e.strerror),
				"%s E:%d" % (name, e.errno))		
	
	
	# FEATURE/440
	# ---
	def exportCLEANSNIPS(self, prefix):
		deleted = 0
		for k in self.snips.keys():
			if k.startswith(prefix):
				self.snips[k].unlink()
				del self.snips[k]
				deleted += 1
		
		if not prefix:
			prefix = "=ALL="
		return (
			(CC.RET_OK, self.SVCID, deleted),
			"%s %d" % (prefix, deleted))
				
			
