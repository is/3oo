#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   First class schedule
#

from __future__ import with_statement

SCHEDULE0_VERSION = '0.0.1.7'

import sys, time
import operator, random
import threading
import Queue

from service import ServiceBase
from protocol import O3Channel
import constants as CC
from utility import D as _D, D2 as _D2, DE as _E, FileLogger

ACTION_WORKSPACEADVERT = 1
ACTION_NEWNODEJOIN = 3
ACTION_STARTMISSION = 19
ACTION_JOBFINISHED = 22 
ACTION_MISSIONFINISHED = 24
ACTION_CLEANMISSION = 25

class NodeInfo(object):
	def __init__(self, id, entry, tag):
		self.id = id
		self.entry = entry
		self.tag = tag
		self.last = {}
		self.currentJob = None
	
class ScheduleCore(threading.Thread):
	def __init__(self, service, server):
		threading.Thread.__init__(self, name = 'SCHEDULE-CORE')
		self.service = service
		self.server = server

		self.nodes = {}
		self.missions = {}
		self.waitQueue = {}

		self.queue = Queue.Queue()
		self.lock = threading.Lock()
		self.needSchedule = False

		self.missionLog = FileLogger('../log/O3Mission')
		self.jobLog = FileLogger('../log/O3Job')

	# ---
	def setup(self, cf): pass
	def secondCheck(self): pass

	# ---
	def acquire(self):
		self.lock.acquire()
	
	def release(self):
		self.lock.release()
		
	# ---
	def createNewMission(self, id, kwargs):
		modname = kwargs.get('module', None)
		__import__(modname)
		mod = sys.modules[modname]

		missionName = kwargs.get('missionclass', 'O3Mission')
		missionclass = getattr(mod, missionName)
		mission = missionclass(id, kwargs)
		mission.setup(kwargs)
		mission.schedule = self
		return mission

	# ---
	# Buggy.....
	# Many structure hold the jobid, not job itself.
	def cleanMission(self, id):
		with self.lock:
			m = self.missions.get(id, None)
			if not m:
				return

			# clean queued jobs
			queued = m.queued
			if len(queued):
				for queue in self.waitQueue.values():
					for j in list(queue):
						if j.id in queued:
							queue.remove(j)
					
			# clean jobs on nodes
			for node in self.nodes.values():
				if node.currentJob:
					if node.currentJob.id in queued:
						node.currentJob = None
					
			# clean mission
			del self.missions[id]
			
	# ---
	def submitMission(self, id, kwargs):
		self.lock.acquire()
		try:
			# Check duplication
			if self.missions.has_key(id):
				return False

			m = self.createNewMission(id, kwargs)
			self.missions[id] = m
			self.queue.put((ACTION_STARTMISSION, id))
			#_D('Submit mission {%s|%s}' % (m.name, m.id), 'S')
			return True

		finally:
			self.lock.release()

	def startMission(self, id):
		self.lock.acquire()
		try:
			m = self.missions[id]
			_D('Mission {%s|%s} start' % (m.name, m.id), 'S')
			m.start()
			self.pushReadyJobsToWaitQueue_(m)
			self.needSchedule = True
			self.schedule_()
		finally:
			self.lock.release()

	# ---
	def workspaceAdvert(self, id, entry, tag, starttime, jobs):
		self.lock.acquire()
		try:
			node = self.nodes.get(id, None)

			if not node:
				node = NodeInfo(id, entry, tag)
				node.last['bron'] = time.time()
				node.last['start'] = starttime
				self.nodes[id] = node
				_D2('WS node up {%s=%08X:%s/%s:%d}' % (
					id, starttime, tag, entry[0], entry[1]))
				self.needSchedule = True
				self.queue.put((ACTION_NEWNODEJOIN, id))

			if jobs:
				node.jobs = jobs
			else:
				node.jobs = None

			node.last['advert'] = time.time()
		finally:
			self.lock.release()
		
	
	# ---
	def schedule(self):
		self.lock.acquire()
		try:
			self.schedule_()
		finally:
			self.lock.release()

	def schedule_(self):
		if not self.needSchedule:
			return

		self.needSchedule = False
		freenodes = [ n for n in self.nodes.values() if 
			n.currentJob == None]
		commonQueue = self.waitQueue.get('common', None)
		
		random.shuffle(freenodes)
		while len(freenodes) != 0:
			node = freenodes.pop()
			queue = self.waitQueue.get(node.id, None)
			if not queue and node.tag == 'normal':
				queue = commonQueue;
			if not queue:
				continue
			queue.sort(key = operator.attrgetter('jobid'))
			job = queue.pop(0)
			self.submitJobToNode_(node, job)

	def submitJobToNode_(self, node, job):
		#_D('SubmitJobToNode_ %s %s' % (node.id, job.jobid))
		node.currentJob = job
		self.runat = node.id
		self.server.delayCall0(self._submitJobToNode, node)
	
	def _submitJobToNode(self, node):
		with self.lock:
			job = node.currentJob
			if job == None:
				_D2('submit job to {%s} canceled' % node.id)
				return
			_D('submit job {%s} to {%s}' % (node.currentJob.jobid, node.id), 'S')
			job.submittime = time.time()
			jobParams = job.getJobParams()
		channel = O3Channel()
		channel.connect(node.entry)
		res = channel(CC.SVC_WORKSPACE, 'STARTJOB', jobParams)
		# TODO Error handler
		channel.close()
	
	# ---
	def missionNotify(self, channel, nodeid, jobid, params):
		self.lock.acquire()
		try:
			node = self.nodes[nodeid]
			mid,jid = jobid.split(':', 1)

			mission = self.missions[mid]
			job = mission.job[jid]

			return mission.notify(channel, node, job, params)
		finally:
			self.lock.release()

	# ---
	def jobFinished(self, nodeid, jobid, params):
		with self.lock:
			node = self.nodes[nodeid]

			mid,jid = jobid.split(':', 1)
			mission = self.missions[mid]
			job = mission.jobs[jid]

			node.currentJob = None
			self.needSchedule = True

			del mission.queued[jid]
			mission.jobFinished(job, params)

			logdetail = []
			logdetail.append('n:%s' % nodeid)
			logdetail.append('r:%.2fs' % (time.time() - job.submittime))
			logdetail.append('w:%.2fs' % (job.submittime - job.createtime))

			if type(params) == dict:
				if params.has_key('insize0'): 
					logdetail.append('i:%.2fm' % params['insize0'])
				if params.has_key('outsize0'): 
					logdetail.append('o:%.2fm' % params['outsize0'])
				#if params.has_key('debuginfo'):
				#	logdetail.append('info:%s' % params['debuginfo'])

			self.jobLog.L('%s %s' % (jobid, ' '.join(logdetail)))

			if type(params) == dict and params.has_key('debuginfo'):
				_D("-JOB-END- %s" % params['debuginfo'])

			for j in job.next:
				j.prev.remove(job)
				j.prevReady.append(job)
				if len(j.prev) == 0:
					self.pushJobToWaitQueue_(mission, j)

			if len(mission.unfinished) + len(mission.queued) == 0:
				self.queue.put((ACTION_MISSIONFINISHED, mission.id))

			self.schedule_()

	def pushReadyJobsToWaitQueue_(self, mission):
		#for job in sorted(mission.unfinished.values(), 
		#	key = operator.attrgetter('jobid')):
		for job in mission.unfinished.values():
			if len(job.prev) == 0:
				self.pushJobToWaitQueue_(mission, job)

	def pushJobToWaitQueue_(self, mission, job):
		del mission.unfinished[job.id]
		mission.queued[job.id] = job

		if job.runat:
			runat = job.runat
		else:
			runat = 'common'

		queue = self.waitQueue.get(runat, None)
		if queue == None:
			queue = list()
			self.waitQueue[runat] = queue
		queue.append(job)
		# TODO sort job by job id or name?

	# ---
	def missionFinished(self, mid):
		with self.lock:
			m = self.missions[mid]
			del self.missions[mid]
			m.finished()

			_D('mission {%s|%s} finished' % (m.name, m.id), 'S')

			# Log to mission logs
			logdetails = []

			jobs = getattr(m, 'jobs', None)
			if jobs: 
				if type(jobs) == int: logdetails.append('jobs:%d' % jobs)
				else: logdetails.append('jobs:%d' % len(jobs))

			size0 = getattr(m, 'insize0', None)
			if size0: logdetails.append('ins:%.2fm' % (size0))

			size0 = getattr(m, 'outsize0', None)
			if size0: logdetails.append('outs:%s.2fm' % (size0))

			starttime = getattr(m, 'starttime', None)
			if starttime: logdetails.append('during:%.2fs' % (time.time() - starttime))

			self.missionLog.L('%s|%s %s' % (
				m.name, m.id, ','.join(logdetails)))
			
	# ---
	def run(self):
		while True:
			try:
				try:
					task = self.queue.get(True, 1)
				except Queue.Empty:
					self.secondCheck()
					continue
	
				if type(task) == tuple or type(task) == list:
					if task[0] == ACTION_WORKSPACEADVERT:
						self.workspaceAdvert(*task[1:])
					elif task[0] == ACTION_JOBFINISHED:
						self.jobFinished(*task[1:])
					elif task[0] == ACTION_STARTMISSION:
						self.startMission(*task[1:])
					elif task[0] == ACTION_NEWNODEJOIN:
						self.schedule()
					elif task[0] == ACTION_MISSIONFINISHED:
						self.missionFinished(*task[1:])
					elif task[0] == ACTION_CLEANMISSION:
						self.cleanMission(*task[1:])
			except Exception, e:
				_E(e)
		
class Schedule0Service(ServiceBase):
	SVCID = CC.SVC_SCHEDULE
	svcDescription = "Schedule service"
	svcName = "SCHEDULE"
	svcVersion = SCHEDULE0_VERSION

	def __init__(self, server):
		self.server = server
		self.core = ScheduleCore(self, server)
	
	def setup(self, cf):
		self.core.setup(cf)
	
	def activate(self):
		self.queue = self.core.queue
		self.core.setDaemon(True)
		self.core.start()

	# ---
	def exportWORKSPACEADVERT(self, channel, nodeid, entry, tag, starttime, jobs):
		self.queue.put((ACTION_WORKSPACEADVERT, nodeid, entry, tag, starttime, jobs))
		return ((CC.RET_OK, self.SVCID, 0), {'dontlog': 1})
	
	def exportLISTWORKSPACES(self, channel, query = None):
		self.core.acquire()
		try:
			res = [(x.id, x.entry, x.tag) for x in self.core.nodes.values()]
		finally:
			self.core.release()
		return (CC.RET_OK, self.SVCID, res)
	
	# TODO detail error message
	def exportSUBMITMISSION(self, channel, id, params):
		if self.core.submitMission(id, params):
			return (CC.RET_OK, self.SVCID, 0)
		else:
			return (CC.RET_ERROR, self.SVCID, 0)
	
	def exportJOBFINISHED(self, channel, nodeid, jobid, params):
		self.queue.put((ACTION_JOBFINISHED, nodeid, jobid, params))
		return ((CC.RET_OK, self.SVCID, 0), "%s %s" % (nodeid, jobid))
	
	def exportMISSIONNOTIFY(self, channel, nodeid, jobid, params):
		return self.core.missionNotify(channel, nodeid, jobid, params)
	
	def exportCLEANMISSION(self, channel, missionid):
		self.queue.put((ACTION_CLEANMISSION, missionid))
		return (CC.RET_OK, self.SVCID, 0)

# -- DOC --
#
# * mission.unfinished - id
# * mission.queued - id
# * mission.finished - id
#
# * waitQueue - job
# * node.currentJob - job

