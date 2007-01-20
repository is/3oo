#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Create WareHouse Index DB
#

DBURL = 'mysql://o3:o3indexdb@p-dx44-in/o3'

import sys
sys.path.append('/is/app/o3/lib/o3')

from sqlalchemy import *

def CreateO3WarehouseDatabase(url):
	engine = create_engine(url)
	metadata = BoundMetaData(engine)

	engine.echo = True

	roomTable = Table(
		'room', metadata,
		Column('id', Integer, primary_key = True),
		Column('zone', Integer, default = 0),
		Column('node', String(20)),
		Column('label', String(20), default = '0'),
		Column('addr', String(40)),
		Column('base', String(20), default = '/data/o3warehouse'),
		Column('capacity', Integer),
		Column('used', Integer, default = 0),
		Column('state', Integer, default = 1),
		Column('last', Integer, nullable = True, default = None),
		Column('active', Integer, nullable = False, default = 0),
		Column('comment', String, nullable = True, default = None),
		UniqueConstraint('node', 'label'),
	)
	roomTable.drop(checkfirst = True)
	roomTable.create(checkfirst = True)

	RoomDB = (
#		('p-cn25', 0, '10.4.170.197', '/data', 60),
#		('p-cn41', 0, '10.4.170.228', '/data1', 120),
#		('p-dx48', 0, '10.6.33.155', '/data', 30),
#		('p-dx47', 0, '10.6.33.154', '/data', 30),
#		('p-dx60', 0, '10.6.39.209', '/data1', 210),
#		('p-dx86', 0, '10.6.39.66', '/data', 100),
#		('p-dx86', 1, '10.6.39.66', '/data1', 210),
		('p-dx53', 0, '10.6.39.202', '/data', 200),
		('p-dx56', 0, '10.6.39.205', '/data1', 200),
		('p-dx58', 0, '10.6.39.207', '/data', 200),
		('p-dx58', 1, '10.6.39.207', '/data1', 200),
		('p-dx61', 0, '10.6.39.210', '/data', 180),
		('p-dx61', 1, '10.6.39.210', '/data1', 180),
	)

	for r in RoomDB:
		roomTable.insert().execute(
			zone = 0, node = r[0], label = str(r[1]),
			addr = r[2], base = '%s/o3warehouse' % r[3],
			capacity = r[4] * 1024 * 1024,
			)

	entityTable = Table(
		'entity', metadata,
		Column('id', Integer, primary_key = True),
		Column('zone', Integer),
		Column('name', String(255)),
		Column('source', String(255)),
		Column('size', Integer),
		Column('mtime', Integer),
		Column('last', Integer),
		Column('mirrors', Integer),
		Column('state', Integer),
		Column('action', Integer),
		Column('tag', String(255), nullable = True, default = None),
		Column('active', Integer, nullable = False, default = 0),
		Column('comment', String(255), nullable = True, default = None),
		UniqueConstraint('name', 'active'),
	)
	entityTable.drop(checkfirst = True)
	entityTable.create(checkfirst = True)

	shadowTable = Table(
		'shadow', metadata,
		Column('id', Integer, primary_key = True),
		Column('entity', Integer, ForeignKey('entity.id')),
		Column('room', Integer, ForeignKey('room.id')),
		Column('mtime', Integer),
		Column('last', Integer),
		Column('taskid', String),
		Column('state', Integer),
		Column('active', Integer, nullable = False, default = 0),
		Column('comment', String),
	)

	shadowTable.drop(checkfirst = True)
	shadowTable.create(checkfirst = True)
	
	engine.dispose()
	

if __name__ == '__main__':
	CreateO3WarehouseDatabase(DBURL)
