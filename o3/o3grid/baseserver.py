#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Base server module
#

BASESERVER_VERSION =  '0.0.2.2'


import threading
import socket, struct, time
import cPickle as pickle
import sys, os
import Queue

import constants as CC
import fastmap

from service import BaseService
from protocol import CreateMessage0, GetMessageFromSocket, CreateMessage
from protocol import O3Call, O3Channel
from utility import D as _D, DE as _E, D2 as _D2
from debuginfo import SVCIDToStr

VERBOSE_CALL = 1

# ------
#def hostid2name(id):
#	if len(id) == 5:
#		return '%s0%s' % (id[:4], id[4])
#	elif len(id) == 7:
#		return '%s%s%s' % (id[:4], chr(ord('a') + int(id[4:6]), chr[6]))
#	return id

# ====
class TimerItem(object):
	def __init__(self, id, interval, repeat, function, args = [], kwargs = {}):
		self.last = time.time()
		self.interval = interval
		self.id = id
		self.function = function
		self.args = args
		self.kwargs = kwargs
		self.repeat = repeat
	
class SecondTimer(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self, name = 'SECONDER')
		self.timer = {}
		self.order = []
		self.finished = threading.Event()
		self.current = time.time()
		self.lock = threading.Lock()
	
	def add(self, id, interval, repeat, function, args = [], kwargs = {}):
		self.lock.acquire()
		timer = TimerItem(id, interval, repeat, function, args, kwargs)
		self.timer[id] = timer
		self.order.append(id)
		self.order.sort()
		self.lock.release()
	
	def remove(self, id):
		self.lock.acquire()
		self.order.remove(id)
		del self.timer[id]
		self.lock.release()

	def run(self):
		while True:
			self.finished.wait(1.0)
			if not self.order:
				continue

			self.lock.acquire()
			try:
				self.current = time.time()
				for t in list(self.order):
					timer = self.timer[t]
					if self.current - timer.last < timer.interval:
						continue
					if timer.repeat:
						timer.last = self.current
						timer.function(*timer.args, **timer.kwargs)
					else:
						del self.timer[t]
						self.order.remove(t)
			finally:
				self.lock.release()

# ====
O3_SERVICES = {
	'workspace': 'workspace/WorkSpace',
	'space': 'space/Space',
	'names': 'names/Name',
	'autoconfig': 'autoconfig/AutoConfig',
	'hub': 'hub/Hub', 
	'schedule': 'schedule0/Schedule0',
	'warehouse': 'warehouse/Warehouse',
}

O3_SERVICES_ORDER = [
	'space', 'workspace', 'names', 'autoconfig', 
	'hub', 'schedule', 'warehouse',
]


# ====
def CommonThreadWorker(queue):
	while True:
		task = queue.get()
		try:
			task[0](*task[1], **task[2])
		except Exception, e:
			_E(e)
			pass

class CommonThreadPool(object):
	def __init__(self, threads, maxsize):
		self.queue = Queue.Queue(maxsize)
		self.pool = []
		self.threads = threads
	
	def start(self):
		for i in range(self.threads):
			thr = threading.Thread(
				name='COMMONWORKER-%d' % i,
				target = CommonThreadWorker, args=(self.queue,))
			thr.setDaemon(True)
			self.pool.append(thr)
			thr.start()

	def addTask(self, func, *args, **kwargs):
		task = [func, args, kwargs]
		self.queue.put(task)

# ====
class ServerBase(object):
	daemonThreads = True
	addressFamily = socket.AF_INET
	socketType = socket.SOCK_STREAM
	requestQueueSize = 50
	allowReuseAddress = True
	verbose = 0
	svcVersion = BASESERVER_VERSION

	def __init__(self):
		self.socket = socket.socket(
			self.addressFamily, self.socketType)
		
		if self.allowReuseAddress:
			self.socket.setsockopt(
				socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.svc = dict()
		self.svcText = dict()
		self.bases = None

	# ----
	def setup(self, config):
		self.cf = config
		self.starttime = int(time.time())

		common = self.cf['common']
		sys.path.append('%s/lib/o3' % common['base'])

		debugopt = common.get('debug')

		if debugopt:
			for key in debugopt.split(','):
				if key == 'call':
					self.verbose |= VERBOSE_CALL
			
		# ID and entry
		self.entry = common.get('entry')
		self.id = common.get('id')
		self.zone = common.get('zone')

		# For Second Timer
		self.second = SecondTimer()

		serveraddr = common.get('listen', None)
		if not serveraddr:
			serveraddr = ('0.0.0.0', common.get('entry')[1])

		self.serverAddress = serveraddr
		self.socket.bind(serveraddr)

		self.localnames = common.get('names', None)
		self.namesaddr = self.localnames['NAMES']

		ulog = common.get('ulog')
		if ulog:
			s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
			s.connect(ulog['addr'])
			sys.modules['o3grid.utility'].LogSetup(
				{'socket': s, 'hostname': self.id})
	
		threads = common.get('threadpoolsize', 5)
		queuesize = common.get('queuesize', 300)

		self.threadPool = CommonThreadPool(threads, queuesize)
		self.threadPool.start()

		_D('O3 Server/%s Listen on %s:%d - [fastmap/%s]' % (self.svcVersion, 
			serveraddr[0], serveraddr[1], fastmap.__REVISION__))

		
	def setupServices(self):
		# Base Service
		bases = BaseService(self)
		bases.setup(self.cf)
		self.registerService(bases)

		for sn in O3_SERVICES_ORDER:
			if not self.cf.has_key(sn):
				continue
			sstr = O3_SERVICES[sn]
			(mname, cname) = sstr.split('/')
			modname = 'o3grid.%s' % mname
			__import__(modname)
			S = getattr(sys.modules[modname], '%sService' % cname)(self)
			S.setup(self.cf)
			self.registerService(S)
			_D('%s__%s service ready' % (S.svcName, S.svcVersion), 'S')
		self.localSpace = self.svc.get(CC.SVC_SPACE, None)


	# ----
	def register(self, svcid, svc, svcText):
		self.svc[svcid] = svc
		self.svcText[svcid] = svcText
		if svcid == CC.SVC_BASE:
			self.bases = svc

	def registerService(self, svc):
		self.register(svc.SVCID, svc, svc.svcDescription)
	
	# ----
	def activate(self):
		self.socket.listen(self.requestQueueSize)

		self.second.setDaemon(True)
		self.second.start()

		for s in self.svc.values():
			s.activate()

	def serveForever(self):
		while True:
			self.handleRequest()
	
	def handleRequest(self):
		(ins, addr) = self.socket.accept()
		
		thr = threading.Thread(
			name = 'BASEWORKER',
			target = self.processRequestThread,
			args = (ins, addr))
		if self.daemonThreads:
			thr.setDaemon(1)
		thr.start()

	# ---
	def processRequestThread(self, ins, addr):
		try:
			while True:
				params = GetMessageFromSocket(ins)
				svcid = params[0]

				retcode = 999 
				retinfo = None
				try:
					try:
						svc = self.svc[svcid]
					except KeyError:
						ins.send(CreateMessage(
							CC.RET_ERROR, CC.SVC_SYSTEM, CC.ERROR_NO_SERVICE))
						retcode = CC.RET_ERROR
						continue
				
					ret = svc.dispatch(ins, params)

					if type(ret[0]) != int:
						retinfo = ret[1]
						ret = ret[0]

					ins.send(CreateMessage0(ret))
					retcode = ret[0]
				except Exception, e:
					_E(e)
					raise e
				finally:
					if self.verbose & VERBOSE_CALL:
						currD = _D
					else:
						currD = _D2

					if retinfo:
						if type(retinfo) == str:
							if retinfo != 'dontlog':
								currD('{%s.%s} P:%d RC:%d + %s' % (
									SVCIDToStr(svcid), params[1], len(params) - 2, retcode, retinfo), 'C')
						elif type(retinfo) == list or type(retinfo) == tuple:
							if retinfo[0] != 'dontlog':
								currD('{%s.%s} P:%d RC:%d + %s' % (
									SVCIDToStr(svcid), params[1], len(params) - 2, retcode, ' '.join(retinfo)), 'C')
						elif not retinfo.has_key('dontlog'):
							currD('{%s.%s} P:%d RC:%d + %s' % (
								SVCIDToStr(svcid), params[1], len(params) - 2, retcode,
								' '.join([ '%s:%s' % (k.split('.', 1)[-1], retinfo[k]) for 
									k in sorted(retinfo)])
								), 'C')
					else:
						currD('{%s.%s} P:%d RC:%d' % (
							SVCIDToStr(svcid), params[1], len(params) - 2, retcode), 'C')
					
		# socket error
		except struct.error:
			return
		finally:
			ins.close()
		return
	
	def resolv(self, name):
		if name.startswith('LOCAL__'):
			name = name[7:]
			return self.localnames.get(name, None)

		if self.localnames.has_key(name):
			return self.localnames[name]

		#channel = O3Channel()
		#channel.connect(self.namesaddr)
		res = O3Call(self.namesaddr, CC.SVC_NAMES, 'RESOLV', name)
		#channel.close()
		return res[2]
	
	# ---
	def addTimer(self, id, interval, repeat, function, args = [], kwargs = {}):
		self.second.add(id, interval, repeat, function, args, kwargs)
	
	def addTimer2(self, id, interval, repeat, function, args = [], kwargs = {}):
		self.second.add(id, interval, repeat, self.delayCall, args = (function, args, kwargs))
	
	def removeTimer(self, id):
		self.second.remove(id)

	def delayCall(self, function, args, kwargs):
		self.threadPool.queue.put((function, args, kwargs))
	
	def delayCall0(self, function, *args, **kwargs):
		self.threadPool.queue.put((function, args, kwargs))
