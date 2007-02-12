#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   DB Layer for Warehouse
#

__all__ = [
	'WarehouseDB',
	'Entity',
	'Room',
]

from sqlalchemy import *

import time

import constants as CC
from utility import appendinmap, removeinmap
from utility import sizeK

class Room(object): pass
class Entity(object): pass
class Shadow(object): pass

class WarehouseDB(object):
	def setup(self, dbURL):
		self.url = dbURL
		engine = create_engine(dbURL)
		metadata = BoundMetaData(engine)

		self.engine = engine
		self.metadata = metadata

		tables = {}
		for i in ('room', 'entity', 'shadow'):
			tables[i] = Table(i, metadata, autoload = True)
		self.tables = tables

		mappers = {}
		mappers['room'] = mapper(Room, tables['room'])
		mappers['entity'] = mapper(Entity, tables['entity'])
		mappers['shadow'] = mapper(Shadow, tables['shadow'])

		self.mappers = mappers
		session = create_session(bind_to = engine)

		self.session = session
		self.qRoom = session.query(Room)
		self.qEntity = session.query(Entity)
		self.qShadow = session.query(Shadow)

		self.room = {}
		self.entity = {}
		self.shadow = {}
		
		self.entityByName = {}
		self.shadowByEntity = {}

		# Load all data from database.
		res = self.qRoom.select_by(active = 0)
		if res:
			for r in res:
				self.room[r.id] = r

		res = self.qEntity.select_by(active = 0)
		if res:
			for r in res:
				self.entity[r.id] = r
				self.entityByName[r.name ] = r

		res = self.qShadow.select_by(active = 0)
		if res:
			for r in res:
				self.shadow[r.id] = r
				appendinmap(self.shadowByEntity, r.entity, r)

	# ---
	def resetDB(self, force):
		self.session.flush()
		self.session.clear()

		self.room.clear()
		self.entity.clear()
		self.shadow.clear()

		self.entityByName.clear()
		self.shadowByEntity.clear()

		# Load all data from database.
		res = self.qRoom.select_by(active = 0)
		if res:
			for r in res:
				self.room[r.id] = r

		res = self.qEntity.select_by(active = 0)
		if res:
			for r in res:
				self.entity[r.id] = r
				self.entityByName[r.name ] = r

		res = self.qShadow.select_by(active = 0)
		if res:
			for r in res:
				self.shadow[r.id] = r
				appendinmap(self.shadowByEntity, r.entity, r)

	# ------
	def flush(self):
		try:
			self.session.flush()
		except:
			self.session.flush()

	def getNodeList(self):
		return list(set([x.node for x in self.room.values()]))

	def getRoomByNode(self, node):
		return [ r for r in self.room.values() if r.node == node ]
	
	# -----
	def addShadow(self, room, entity): 
		s = Shadow()
		s.room = room.id
		s.entity = entity.id
		s.mtime = entity.mtime
		s.state = CC.ENTITY_STATE_INIT
		s.active = 0
		s.last = int(time.time())

		self.session.save(s)
		self.flush()

		self.shadow[s.id] = s
		appendinmap(self.shadowByEntity, entity.id, s)
		return s

	# ------
	def addRoom(self, ri):
		node = ri['node']
		label = ri['label']

		rooms = [ r for r in self.room.values() if 
			r.node == node and r.label == label ]

		if len(rooms) != 0:
			return None

		room = Room()
		room.node = node
		room.label = ri['label']
		room.zone = ri.get('zone', 0)
		room.addr = ri['addr']
		room.base = ri['base']
		room.capacity = ri['capacity']
		room.used = 0
		room.state = 1
		room.last = int(time.time())
		room.active = 0

		self.session.save(room)
		self.session.flush()
		self.room[room.id] = room

		return room
		
	# ------
	def addEntity(self, ei):
		name = ei.get('name')
		if self.entityByName.has_key(name):
			return CC.ERROR_WAREHOUSE_DUPLICATION_NAME

		e = Entity()
		e.name = ei.get('name')
		e.zone = ei.get('zone', 0)
		e.source = '%s:%s,%d:_Z/%s' % (
			ei['node'], ei['entry'][0], ei['entry'][1], ei['path'])
		e.size = ei.get('size')
		e.mtime = ei.get('mtime')
		e.state = CC.ENTITY_STATE_INIT
		e.active = 0
		e.mirrors = ei.get('mirrors', 2)

		e.comment = ei.get('comment', None)
		e.tag = ei.get('tag', None)

		self.session.save(e)
		#self.session.flush()
		self.flush()

		self.entity[e.id] = e
		self.entityByName[e.name] = e
		return e
	
	# ===
	def setEntityInfo(self, eid, info):
		if type(eid) == str:
			e = self.entityByName.get(eid, None)
		elif type(eid) == int or type(eid) == long:
			e = self.entity.get(eid, None)
		else:
			e = None

		if not e:
			return CC.ERROR_NO_SUCH_OBJECT

		if e.active != 0:
			return CC.ERROR_NO_SUCH_OBJECT

		for k in ('source', 'tag', 'label', 'comment', 'mtime'):
			if info.has_key(k):
				setattr(e, k, info[k])

		# size -- need update all shadows' room's used value
		if info.has_key('size'):
			shadows = self.shadowByEntity.get(e.id, None)
			if shadows:
				size0 = sizeK(e.size)
				size1 = sizeK(info['size'])

				for room in [ self.room[s.room] for s in 
					shadows if s.active == 0 ]:
					room.used -= size0
					room.used += size1
			e.size = info['size']
		self.flush()
		return 0

	# ===
	def getEntity(self, en):
		if type(en) == int or type(en) == long:
			return self.entity.get(en, None)

		entity = self.entityByName.get(en, None)
		if entity:
			return entity

		try:
			return self.entity.get(int(en), None)
		except:
			return None
			
