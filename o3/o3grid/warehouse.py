#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Warehouse server in O3 grids
#

from __future__ import with_statement

WAREHOUSE_VERSION = '0.0.0.27'

import sys, os, time
import threading
from random import choice, random

import constants as CC
from service import ServiceBase
from protocol import O3Channel, O3Call
from warehousedb import WarehouseDB

from utility import appendinmap, removeinmap, leninmap, FileLogger
from utility import sizeK, RoomLocation, RoomLocationToTuple
from utility import D as _D, D2 as _D2, DE as _E

class NodeInfo(object):
	def __init__(self, id, entry = None):
		self.id = id
		self.entry = entry
		self.last = {}
		self.last['start'] = 0
		self.last['advert'] = time.time()
		self.tasks = []
		self.state = CC.NODE_STATE_INIT

class WarehouseService(ServiceBase):
	SVCID = CC.SVC_WAREHOUSE
	svcDescription = "WAREHOUSE service"
	svcName = 'WAREHOUSE'
	svcVersion = WAREHOUSE_VERSION

	def __init__(self, server):
		self.server = server
		self.lock = threading.Lock()
		self.disableCheck = False
		self.entityLog = FileLogger('../log/O3Entity')
	
	def setup(self, cf):
		cf = cf['warehouse']
		self.db = WarehouseDB()
		self.db.setup(cf['dburl'])

		self.nodes = {}

		for n in set([r.node for r in self.db.room.values()]):
			self.nodes[n] = NodeInfo(n)

		self.taskByEntity = {}
		self.tasks = {}

		self.server.addTimer2('warehouse_cyclecheck', 1, True, self.mainCheck)
		self.actionlast = {}
		for i in ('entityone', 'cleanoldobject',
			'nodeoffline', 'flushdb'):
			self.actionlast[i] = int(time.time()) + 10

	# ------
	def mainCheck(self):
		with self.lock:
			if self.disableCheck:
				return

			cur = int(time.time())
			last = self.actionlast

			if cur - last['entityone'] > 2:
				last['entityone'] = cur
				self.checkEntityOne_()

			if cur - last['cleanoldobject'] > 20:
				last['cleanoldobject'] = cur
				self.cleanObjectInDB_()

			if cur - last['nodeoffline'] > 5:
				last['nodeoffline'] = cur
				self.checkNodeOffline_()

			if cur - last['flushdb'] > 60:
				last['flushdb'] = cur
				self.flushDB_()

	# -----
	def registerOuterTask_(self, task):
		entity = self.db.entity.get(
			task.get('entityid', None), None)
		room = self.db.room.get(
			task.get('roomid', None), None)
		taskid = task.get('taskid')

		self.tasks[taskid] = task
		if room:
			self.nodes[room.node].tasks.append(taskid)
		if entity:
			appendinmap(self.taskByEntity, entity.id, taskid)
	
	def unregisterOuterTask_(self, task):
		entity = self.db.entity.get(
			task.get('entityid', None), None)
		room = self.db.room.get(
			task.get('roomid', None), None)
		taskid = task.get('taskid')

		del self.tasks[taskid]
		if room:
			self.nodes[room.node].tasks.remove(taskid)
		if entity:
			removeinmap(self.taskByEntity, entity.id, taskid)

	def arrange_(self, **task):
		if task['task'] == 'MORESHADOW':
			return self.moreMirror_(task)
		print 'ARRANGE_END'
	
	# ---
	def moreMirror_(self, task):
		entity = task['entity']

		if entity.state > CC.ENTITY_STATE_READY:
			return False

		# Check taskByEntity
		if leninmap(self.taskByEntity, entity.id) >= 1:
			return False

		shadows = self.db.shadowByEntity.get(entity.id, '')

		if len(shadows) >= entity.mirrors:
			if len([s for s in shadows if 
				s.state == CC.SHADOW_STATE_OK or
				s.state == CC.SHADOW_STATE_MIRROR ]) >= entity.mirrors:

				if entity.state == CC.ENTITY_STATE_SHADOWING:
					entity.state = CC.ENTITY_STATE_READY
					self.db.flush()
				return False

		room = self.allocateRoom0_(entity)

		if not room:
			return False

		size = sizeK(entity.size)
		sourceshadow = None

		if leninmap(self.db.shadowByEntity, entity.id) != 0:
			for s in self.db.shadowByEntity[entity.id]:
				room0 = self.db.room[s.room]
				node0 = self.nodes[room0.node]

				if s.state == CC.SHADOW_STATE_OK and \
					room0.state == CC.ROOM_STATE_OK and \
					node0.state == CC.NODE_STATE_ONLINE:
					if sourceshadow:
						if sourceshadow.id < s.id:
							sourceshadow = s
					else:
						sourceshadow = s

		shadow = self.db.addShadow(room, entity)
		room.used += size
		res = self.mirrorShadow_(sourceshadow, shadow)
		entity.state = CC.ENTITY_STATE_SHADOWING
		self.db.flush()
		return res

	# ===
	def dropEntity_(self, entityid): 
		res = None
		entity = self.db.entity[entityid]

		if leninmap(self.db.shadowByEntity, entityid) != 0:
			shadows = self.db.shadowByEntity[entityid]
			res = []

			for s in shadows:
				if s.state <= CC.SHADOW_STATE_OK:
					s.state = CC.SHADOW_STATE_DROPED

					room = self.db.room[s.room]
					if self.nodes[room.node].state != CC.NODE_STATE_ONLINE:
						continue

					addr = room.addr
					label = room.label
					name = entity.name
					res.append((addr, label, name))
		
		entity.state = CC.ENTITY_STATE_DROPED
		del self.db.entityByName[entity.name]
		self.db.flush()
		return res

	def dropRoom_(self, roomid): pass
	# ===

	
	# ===
	def mirrorShadow_(self, src, dst):
		entity = self.db.entity[dst.entity]
		droom = self.db.room[dst.room]
		dentry = (droom.addr, CC.DEFAULT_WAREHOUSE_PORT)

		if src:
			sroom = self.db.room[src.room]
			sentry = (sroom.addr, CC.DEFAULT_PORT)
			srcloc = RoomLocation(sroom.node, 
				(sroom.addr, CC.DEFAULT_WAREHOUSE_PORT), 
				sroom.label, entity.name)
			srcnode = sroom.node
			srclabel = sroom.label
		else:
			srcloc = entity.source
			srcnode, sentrystr, srcpath = srcloc.split(':', 2)

			saddr, sport = sentrystr.split(',')
			sentry = (saddr, int(sport))
			srclabel = srcpath.split('/', 1)[0][1:]

		taskid = 'RM-%d-%d' % (droom.id, dst.id)

		task = {
			'action': 'MIRROR',
			'taskid': taskid,
			'shadowid': dst.id,
			'entityid': entity.id,
			'roomid': droom.id,
			'source': srcloc,
			'destroomlabel': droom.label,
			'name': entity.name,
			'size': entity.size,
			'mtime': entity.mtime,
			'starttime': int(time.time()),
		}

		_D2('entity mirror {%d=R%d-E%d,name:%s,from:%s/%s,to:%s/%s}' % (
			dst.id, droom.id, entity.id, entity.name,
			srcnode, srclabel, droom.node, droom.label))
		self.entityLog.L('SM E%d=S%d %s:%s/%s %s/%s' % (
			entity.id, dst.id, droom.node, droom.label, entity.name,
			srcnode, srclabel))

		# TODO Error handler
		S = O3Channel().connect(dentry)
		res = S(CC.SVC_SPACE, 'ROOMMIRROR', task)
		S.close()

		if res[0] == CC.RET_OK:
			dst.state = CC.SHADOW_STATE_MIRROR
			dst.last = time.time()
			self.registerOuterTask_(task)
			return True
		else:
			dst.state = CC.SHADOW_STATE_UNUSED
			dst.last = time.time()
		return False


	def allocateRoom0_(self, entity):
		rooms = self.db.room
		size = sizeK(entity.size)
		
		shadows = self.db.shadowByEntity.get(entity.id, None)
		if shadows:
			nodes = set([ rooms[s.room].node for s in shadows ])
		else:
			nodes = []

		mintasks = 4
		arooms = []
		sumspace = 0

		for r in rooms.values():
			if r.state != CC.ROOM_STATE_OK:
				continue
			freespace = r.capacity - r.used
			if freespace < size:
				continue

			if r.node in nodes:
				continue

			node = self.nodes[r.node]
			if node.state != CC.NODE_STATE_ONLINE:
				continue

			if len(node.tasks) > mintasks:
				continue

			if len(node.tasks) == mintasks:
				sumspace += freespace
				arooms.append((r, freespace))
				continue

			mintasks = len(node.tasks)
			sumspace = freespace
			arooms = [(r, freespace)]

		#arooms = [ r for r in rooms.values() if
		#	r.state == CC.ROOM_STATE_OK and 
		#	r.capacity - r.used > size and 
		#	r.node not in nodes and
		#	self.nodes[r.node].state == CC.NODE_STATE_ONLINE and
		#	len(self.nodes[r.node].tasks) < 3 ]

		if len(arooms) == 0:
			return None

		selector = random() * sumspace
		for x in arooms:
			selector -= x[1]
			if selector <= 0:
				return x[0]
		return arooms[-1][0]

	# ---
	def cleanNodeTasks_(self, node):
			if len(node.tasks):
				for x in list(node.tasks):
					if x.startswith('RM-'):
						taskinfo = self.tasks[x]
						shadow = self.db.shadow[taskinfo['shadowid']]
						shadow.state = CC.SHADOW_STATE_FAILED
						self.db.flush()
					self.unregisterOuterTask_(x)

	def cleanObjectInDB_(self):
		session = self.db.session
		cur = int(time.time())
		fobj = []

		# Clean room
		rooms = [ r for r in self.db.room.values() if 
			r.state == CC.ROOM_STATE_DROPED ]
		for r in rooms:
			r.active = r.id
			r.last = cur
			fobj.append(r)
			_D('_CLEANDB_ remove room {%s=%d/_%s}' % (r.node, r.id, r.label))
			del self.db.room[r.id]

		# Clean Nodes
		if len(rooms) != 0:
			nodes = set([ r.node for r in self.db.room.values() ])
			for node in self.nodes:
				if node not in nodes:
					_D('_CLEANDB_ remove room node {%s=%d}' % (
						self.nodes[node].id, self.nodes[node].last['start']))
				del self.nodes[node]

		# Clean entity
		# for e in [e for e in self.db.entity.values() if e.state == CC.ENTITY_STATE_DROPED]:
		#	print '%d: %d' % (e.id, leninmap(self.db.shadowByEntity, e.id))

		entitys = [ e for e in self.db.entity.values() if 
			(e.state == CC.ENTITY_STATE_ILL or
			e.state == CC.ENTITY_STATE_DROPED) and
			leninmap(self.db.shadowByEntity, e.id) == 0]
		
		for e in entitys:
			e.active = e.id
			e.last = cur
			fobj.append(e)
			#session.flush()
			#session.expunge(e)
			del self.db.entity[e.id]
			try: del self.db.shadowByEntity[e.id]
			except: pass
			try: del self.taskByEntity[e.id]
			except: pass
			_D2('_CLEANDB_ remove entity {%d=%s}' % ( e.id, e.name))

		# Clean shadow
		shadows = [ s for s in self.db.shadow.values() if 
			s.state == CC.SHADOW_STATE_FAILED or
			s.state == CC.SHADOW_STATE_DROPED ]
		for s in shadows:
			room = self.db.room[s.room]
			entity = self.db.entity[s.entity]

			room.used -= sizeK(entity.size)
			s.active = s.id 
			s.last = cur
			fobj.append(s)

			del self.db.shadow[s.id]
			removeinmap(self.db.shadowByEntity, s.entity, s)
			_D2('_CLEANDB_ remove shadow {S%d:E%d=%s}' % (
				s.id, s.entity, self.db.entity[s.entity].name))

		if fobj:
			_D('_CLEANDB_ clean %d objects' % len(fobj))
			self.db.flush()
			for o in fobj:
				session.expunge(o)

	
	def checkEntityOne_(self):
		actions = 20
		for e in self.db.entity.values():
			if actions == 0:	
				return

			if leninmap(self.taskByEntity, e.id) != 0:
				continue

			if leninmap(self.db.shadowByEntity, e.id) < e.mirrors:
				self.arrange_(task = 'MORESHADOW', entity = e)
				actions -= 1
				continue

			def roomAlive(s):
				room = self.db.room[s.room]
				node = self.nodes[room.node]

				if room.state == CC.ROOM_STATE_OK or \
					room.state == CC.ROOM_STATE_LOCK:
					return True
				else:
					return False

			shadows = [ s for s in self.db.shadowByEntity[e.id] if
				(s.state == CC.SHADOW_STATE_OK or
				s.state == CC.SHADOW_STATE_MIRROR) and 
				roomAlive(s)]

			if len(shadows) < e.mirrors:
				self.arrange_(task = 'MORESHADOW', entity = e)
				actions -= 1
				continue
	

	# ---
	def checkNodeOffline_(self):
		cur = time.time()
		for node in self.nodes.values():
			if node.state != CC.NODE_STATE_OFFLINE and cur - node.last['advert'] > 40:
				node.state = CC.NODE_STATE_OFFLINE
				_D('room offline {%s=%08X}' % (node.id, node.last['start']))
	
	# ===
	def flushDB_(self):
		self.db.flush()

	def resetDB_(self, force = False):
		self.db.resetDB(force)
		return 0

	# ===
	def exportFLUSHDB(self, channel):
		with self.lock:
			self.db.flush()
			return (CC.RET_OK, self.SVCID, 0)
	# ===
	def exportRESETDB(self, channel, force = False):
		with self.lock:
			res = self.resetDB_(force)
			return (CC.RET_OK, self.SVCID, 0)

	# ===
	def exportGETENTITYSHADOW(self, channel, entityid):
		with self.lock:
			shadows = self.db.shadowByEntity.get(entityid, None)
			if not shadow:
				return (CC.RET_OK, self.SVCID, 0, 0)

			readyshadows = [ s for s in shadows if s.state == CC.SHADOW_STATE_OK ]
			return (CC.RET_OK, self.SVCID, len(readyshadows), len(shadows))

	# ---
	def exportGETACTIVETASKS(self, channel):
		with self.lock:
			return (
				(CC.RET_OK, self.SVCID, len(self.tasks)), 'dontlog')

	def exportGETACTIVETASKSBYSOURCENODE(self, channel, node):
		with self.lock:
			tasks = []
			for i in self.tasks:
				if i.startswith('RM-'):
					task = self.tasks[i]
					snode = RoomLocationToTuple(task['source'])[0]
					if snode == node:
						tasks.append(i)
			return (
				(CC.RET_OK, self.SVCID, tasks), 'dontlog')
	
	# ---
	def exportAUTOCONFIG(self, channel, zone, nodeid):
		with self.lock:
			rooms = self.db.getRoomByNode(nodeid)
			if rooms == None:
				return (CC.RET_OK, self.SVCID, None)

			cf = {'rooms': [ (x.id, x.label, x.base, x.capacity, x.used) for x in rooms ]}
			return (CC.RET_OK, self.SVCID, cf)

	# ---
	def exportLISTALLNODE(self, channel):
		with self.lock:
			return (CC.RET_OK, self.SVCID, self.db.getNodeList())
	
	# ---
	def exportADDENTITY(self, channel, einfo): 
		with self.lock:
			res = self.db.addEntity(einfo)
			if type(res) == int:
				return (CC.RET_ERROR, self.SVCID, res)

			_D('add entity {%d=%s:%s} size=%.2fM' % (
				res.id, einfo['node'], einfo['path'], res.size / 1024.0 / 1024))
			self.entityLog.L('EA E%d=%s %.2fm' % (res.id, res.name, res.size / 1024 / 1024))
				
				
			self.arrange_(task = 'MORESHADOW', entity = res)
			return (CC.RET_OK, self.SVCID, res.id)

	# ---
	def exportLISTROOM(self, channel):
		with self.lock:
			rooms = [
				[room.id, room.node, room.label, room.base, 
				room.capacity, room.used, room.state] for room in self.db.room.values() ]
			return (CC.RET_OK, self.SVCID, rooms)

	def exportCLEANROOM(self, channel, roomid):
		with self.lock:
			room = self.db.room[roomid]
			shadows = [ s for s in self.db.shadow.values() if s.room == room.id and 
				s.state <= CC.SHADOW_STATE_OK ]
			names = [ self.db.entity[s.entity].name for s in shadows ]
		
		entry = (room.addr, CC.DEFAULT_PORT)
		S = O3Channel().connect(entry)
		res = S(CC.SVC_SPACE, 'ROOMCLEAN', room.label, names)
		S.close()
		return (CC.RET_OK, self.SVCID, res[2])
	
	def exportCHECKROOMSHADOW(self, channel, roomid):
		with self.lock:
			room = self.db.room.get(roomid, None)
			if not room:
				return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

			if self.nodes[room.node].state != CC.NODE_STATE_ONLINE:
				return (CC.RET_ERROR, self.SVCID, ERROR_NETWORK)

		res = O3Call((room.addr, CC.DEFAULT_PORT), CC.SVC_SPACE, 'ROOMSHADOWLIST', room.label)
		if res[0] != CC.RET_OK:
			return (CC.RET_ERROR, self.SVCID, res[2])

		with self.lock:
			count = 0
			exists = set(res[2])
			roomused = 0
			for shadow in [ s for s in self.db.shadow.values() if 
				s.room == room.id and
				s.state == CC.SHADOW_STATE_OK ]:

				entity = self.db.entity[shadow.entity]
				if entity.name not in exists:
					_D2('missing entity {%d=R%d-E%d:%s/_%s:%s}' % (
						shadow.id, room.id, shadow.entity, room.node, room.label,
						entity.name))
					count += 1
					shadow.state = CC.SHADOW_STATE_FAILED
				else:
					roomused += sizeK(entity.size)

			if room.used != roomused:
				room.used = roomused
			self.db.flush()
		return (CC.RET_OK, self.SVCID, count)

	# ---
	def exportDROPENTITY(self, channel, entityid):
		with self.lock:
			if type(entityid) == str:
				entity = self.db.entityByName.get(entityid, None)
				if not entity:
					return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)
				entityid = entity.id
			elif not self.db.entity.has_key(entityid):
				return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)
			res = self.dropEntity_(entityid)

		if res:
			# DROP SHADOW directly
			for loc in res:
				S = O3Channel()
				try:
					S.connect((loc[0], CC.DEFAULT_WAREHOUSE_PORT))
					S(CC.SVC_SPACE, 'ROOMDROPSHADOW', loc[1], loc[2])
				except:
					pass
				finally:
					S.close()
						
		return (CC.RET_OK, self.SVCID, 0)
							

	# ---
	def exportADDROOM(self, channel, roominfo): 
		with self.lock:
			room = self.db.addRoom(roominfo)

			if room == None:
				return (CC.RET_ERROR, self.SVCID, 0)

			if not self.nodes.has_key(room.node):
				self.nodes[room.node] = NodeInfo(room.node)
			_D2('add room {%d=%s:%s:%s:%d}' % (
				room.id, room.node, room.id, room.base, room.capacity))

			S = O3Channel().connect((room.addr, CC.DEFAULT_PORT))
			res = S(CC.SVC_SPACE, 'ROOMADDCONFIG', (
				room.id, room.label, room.base, room.capacity, room.used))
			S.close()
			return (CC.RET_OK, self.SVCID, room.id)

	def exportDROPROOM(self, channel, roomid):
		with self.lock:
			if not self.db.room.has_key(roomid):
				return (CC.RET_ERROR, self.SVCID, ERROR_NO_SUCH_OBJECT)
			self.dropRoom_(self, roomid)
			return (CC.RET_OK, self.SVCID, ERROR_NO_SUCH_OBJECT)

	# ---
	def exportARRANGE(self, channel, task):
		with self.lock:
			res = self.arrange_(task)
			return (CC.RET_OK, self.SVCID, res)

	# ---
	def exportMIRRORFINISHED(self, channel, taskinfo):
		with self.lock:
			shadow = self.db.shadow[taskinfo['shadowid']]
			entity = self.db.entity[taskinfo['entityid']]

			result = taskinfo['result']
			if result == 0 and shadow.state == CC.SHADOW_STATE_MIRROR:
				shadow.state = CC.SHADOW_STATE_OK
			else:
				shadow.state = CC.SHADOW_STATE_FAILED
			self.db.flush()

			try:
				self.unregisterOuterTask_(taskinfo)
				self.arrange_(task = 'MORESHADOW', entity = entity)
			except:
				pass
			
			return ((CC.RET_OK, self.SVCID, 0), 'dontlog')
	# ---
	def exportROOMADVERT(self, channel, nodeid, entry, starttime, tasks, rooms):
		with self.lock:
			node = self.nodes.get(nodeid, None)

			if not node:
				node = NodeInfo(nodeid)
				self.nodes[nodeid] = node
				_D('WH empty node up {%s=%08X:%s:%d}' % (
					nodeid, starttime, entry[0], entry[1]))
				node.entry = entry
				node.last['start'] = starttime
				node.last['born'] = time.time()
				node.last['advert'] = time.time()
				node.state = CC.NODE_STATE_ONLINE;
				return ((CC.RET_OK, self.SVCID, 0), 'dontlog')
				#return (CC.RET_ERROR, self.SVCID, CC.ERROR_NO_SUCH_OBJECT)

			if node.last['start'] == 0:
				node.last['born'] = time.time()
				node.last['start'] = starttime
				node.entry = entry
				self.nodes[id] = node
				_D('WH node up {%s=%08X:%s/%s:%d}' % (
					nodeid, starttime, 
					','.join([str(r) for r in rooms]),
					entry[0], entry[1]))
				node.last['advert'] = time.time()
				node.state = CC.NODE_STATE_ONLINE
				return ((CC.RET_OK, self.SVCID, 0), 'dontlog')

			if node.last['start'] == starttime:
				node.last['advert'] = time.time()
				if node.state != CC.NODE_STATE_ONLINE:
					_D('WH node online {%s=%08X}' % (node.id, node.last['start']))
					node.state = CC.NODE_STATE_ONLINE
				return ((CC.RET_OK, self.SVCID, 0), 'dontlog')

			# room restarted
			_D('WH node restart {%s=%08X:%s/%s:%d}' % (
				nodeid, starttime,
				','.join([str(r) for r in rooms]),
				entry[0], entry[1]))

			self.cleanNodeTasks_(node)
			node.last['start'] = starttime
			node.last['advert'] = time.time()
			return ((CC.RET_OK, self.SVCID, 0), 'dontlog')

	# ===
	def exportLISTENTITY0(self, channel, name):
		with self.lock:
			result = [[e.id, e.name, e.mtime, e.size] for e in
				self.db.entity.values() if 
				e.state == CC.ENTITY_STATE_READY and 
				e.name.startswith(name)]
			return ((CC.RET_OK, self.SVCID, result),
				'query:%s entity:%d' % (name, len(result)))

	# ===
	def exportLISTENTITY1(self, channel, name):
		with self.lock:
			result = [ {'id':e.id, 'name': e.name, 'mtime': e.mtime, 'size':e.size} for e in
				self.db.entity.values() if
				e.state == CC.ENTITY_STATE_READY and
				e.name.startswith(name)]

			return ((CC.RET_OK, self.SVCID, result),
				'query:%s entity:%d' % (name, len(result)))

	# ===
	def exportLISTENTITYLOCATION0(self, channel, eids):
		with self.lock:
			result = {}
			for e in eids:
				entity = self.db.getEntity(e)
				if not entity:
					result[e] = None
				else:
					shadows = self.db.shadowByEntity.get(entity.id, None)
					if not shadows:
						result[e] = None
					else:
						result[e] = [ (s.id, 
							self.db.room[s.room].node, 
							self.db.room[s.room].addr,
							self.db.room[s.room].label,
							entity.name,
							entity.size) for s in shadows if s.state == CC.SHADOW_STATE_OK]
						if len(result[e]) == 0:
							result[e] = None

			return (CC.RET_OK, self.SVCID, result)
	# ===
	def exportLISTENTITYLOCATION1(self, channel, eids):
		with self.lock:
			result = {}
			for e in eids:
				entity = self.db.getEntity(e)
				if not entity:
					result[e] = None
				else:
					shadows = self.db.shadowByEntity.get(entity.id, None)
				if not shadow:
					result[e] = None
				else:
					result[e] = [ {
						'id': s.id, 
						'node': self.db.room[s.room].node,
						'addr':self.db.room[s.room].addr,
						'label': self.db.room[s.room].label,
						'name': entity.name,
						'size': entity.size } for s in
						shadows if s.state == CC.SHADOW_STATE_OK ]
					if len(result[e]) == 0:
						result[e] = None
		return (CC.RET_OK, self.SVCID, result)
