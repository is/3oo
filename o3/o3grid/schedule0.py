#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   First class schedule
#

from __future__ import with_statement

SCHEDULE0_VERSION = '0.0.1.8'

import sys, time
import operator, random
import threading
import Queue

from service import ServiceBase
from protocol import O3Channel
import constants as CC
from utility import D as _D, D2 as _D2, DE as _E, FileLogger

# ===
ACTION_WORKSPACEADVERT = 1
ACTION_NEWNODEJOIN = 3
ACTION_STARTMISSION = 19
ACTION_JOBFINISHED = 22 
ACTION_MISSIONFINISHED = 24
ACTION_CANCELMISSION = 25

# ===
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
		self.serial = 0

		self.queue = Queue.Queue()
		self.lock = threading.Lock()
		self.needSchedule = False

		self.missionLog = FileLogger('../log/O3Mission')
		self.jobLog = FileLogger('../log/O3Job')

		self.missionHistory = []
		self.missionHistorySize = 120

	# ---
	def setup(self, cf): pass
	def secondCheck(self): pass

	# ---
	def acquire(self):
		self.lock.acquire()
	
	def release(self):
		self.lock.release()
		
	# ---
	def createMission_(self, kwargs):
		P = kwargs
		priority = P.get('priority', '6')
		id = '%s%05d' % (priority, self.serial)
		self.serial += 1

		modname = P.get('module', None)
		__import__(modname)
		mod = sys.modules[modname]
		missionClassName = P.get('missionclass', 'O3Mission')
		MissionClass = getattr(mod, missionClassName)
		mission = MissionClass(id, P)
		mission.setup(P)
		mission.schedule = self
		return mission

	# ---
	def cancelMission_(self, id):
		mission = self.missions.get(id, None)
		# mission wasn't found
		if not mission: 
			return
			
		# mission wasn't in active state
		if mission.state not in (CC.SMISSION_READY, CC.SMISSION_DOING):
			return

		# clean jobs in mission.waitJob
		mission.waitJobs.clear()

		# clean jobs in mission.readyJobs
		if len(mission.readyJobs):
			readyJobIDs = mission.readyJobs.keys()
			for queue in self.waitQueue.values():
				for job in list(queue):
					if job.mission == mission and job.id in readyJobIDs:
						queue.remove(job)
			mission.readyJobs.clear()

		# clean jobs in mission.runJobs:
		if len(mission.runJobs):
			for job in mission.runJobs.values():
				node = self.nodes.get(job.runat, None)
				if not node:
					continue
				node.currentJob = None
			mission.runJobs.clear()

		mission.state = CC.SMISSION_CANCEL
		del self.missions[id]
		self.pushToMissionHistory_(mission)

	# --- cancelMission
	def cancelMission(self, id):
		with self.lock:
			self.cancelMission_(id)

	# ---
	def pushToMissionHistory_(self, mission):
		if len(self.missionHistory) > self.missionHistorySize:
			self.missionHistory.pop(0)
		self.missionHistory.append(mission)
		
	# ---
	def submitMission(self, kwargs):
		self.lock.acquire()
		try:
			m = self.createMission_(kwargs)
			self.missions[m.id] = m
			self.queue.put((ACTION_STARTMISSION, m.id))
			return m.id

		finally:
			self.lock.release()

	def startMission(self, id):
		self.lock.acquire()
		try:
			m = self.missions[id]
			_D('mission {%s|%s} start' % (m.name, m.id), 'S')
			m.prepare()
			m.state = CC.SMISSION_READY
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
		#_D('submitJobToNode_ %s %s' % (node.id, job.jobid))
		node.currentJob = job
		del job.mission.readyJobs[job.id]
		job.mission.runJobs[job.id] = job
		job.runat = node.id
		job.state = CC.SJOB_SUBMIT
		self.server.delayCall0(self._submitJobToNode, node)
	
	def _submitJobToNode(self, node):
		with self.lock:
			job = node.currentJob
			if job == None:
				_D2('cancel submited job at {%s}' % node.id)
				return
			_D('submit %s|%s:%s to %s' % (
				job.jobid, job.mission.name, job.name, node.id), 'S')
			job.state = CC.SJOB_RUN
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
	def jobFinished(self, nodeid, jobid, res, info):
		with self.lock:
			node = self.nodes[nodeid]

			mid,jid = jobid.split(':', 1)
			#mission = self.missions[mid]

			node.currentJob = None
			self.needSchedule = True

			mission = self.missions.get(mid, None)
			if mission == None:
				_D('mission %s cancelled' % (jobid))
				self.schedule_()
				return

			job = mission.jobs[jid]
			del mission.runJobs[jid]

			job.state = CC.SJOB_FINISHED

			if info.has_key('exception'):
				_D('job exception raised %s %s:%s' % (
					nodeid, jobid, info['exception']['typename']))
				self.cancelMission_(mid)
				mission.state = CC.SMISSION_EXCEPTION
				self.schedule_()
				return

			try:
				mission.jobFinished(job, res)
			except Exception, e:
				_D('job-finished exception raised %s:%s' % (
					jobid, e.__class__.__name__))
				self.cancelMission_(mid)
				mission.state = CC.SMISSION_EXCEPTION
				_E(e)
				return

			logdetail = []
			logdetail.append('n:%s' % nodeid)
			logdetail.append('r:%.2fs' % (time.time() - job.submittime))
			logdetail.append('w:%.2fs' % (job.submittime - job.createtime))

			if type(res) == dict:
				if res.has_key('insize0'): 
					logdetail.append('i:%.2fm' % res['insize0'])
				if res.has_key('outsize0'): 
					logdetail.append('o:%.2fm' % res['outsize0'])
				#if res.has_key('debuginfo'):
				#	logdetail.append('info:%s' % res['debuginfo'])

			self.jobLog.L('%s|%s:%s %s' % (
				jobid, mission.name, job.name, ' '.join(logdetail)))

			if type(res) == dict and res.has_key('debuginfo'):
				_D("job-end %s" % res['debuginfo'])

			for j in job.next:
				j.prev.remove(job)
				j.prevReady.append(job)
				if len(j.prev) == 0:
					self.pushJobToWaitQueue_(mission, j)

			#if len(mission.unfinished) + len(mission.queued) == 0:
			#	self.queue.put((ACTION_MISSIONFINISHED, mission.id))
			if len(mission.waitJobs) + len(mission.readyJobs) + len(mission.runJobs) == 0:
				self.queue.put((ACTION_MISSIONFINISHED, mission.id))

			self.schedule_()

	def pushReadyJobsToWaitQueue_(self, mission):
		for job in mission.waitJobs.values():
			if len(job.prev) == 0:
				self.pushJobToWaitQueue_(mission, job)

	def pushJobToWaitQueue_(self, mission, job):
		# Move job from wait queue to ready queue
		del mission.waitJobs[job.id]
		mission.readyJobs[job.id] = job
		job.state = CC.SJOB_READY

		# Push job in global submitting Queue
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
			m.state = CC.SMISSION_DONE

			self.pushToMissionHistory_(m)
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
				m.id, m.name, ','.join(logdetails)))
			
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
					elif task[0] == ACTION_CANCELMISSION:
						self.cancelMission(*task[1:])
			except Exception, e:
				_E(e)
			self.queue.task_done()
		
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
	def exportSUBMITMISSION(self, channel, params):
		mid = self.core.submitMission(params)
		if mid:
			return (CC.RET_OK, self.SVCID, mid)
		else:
			return (CC.RET_ERROR, self.SVCID, 0)
	
	def exportJOBFINISHED(self, channel, nodeid, jobid, res, info):
		self.queue.put((ACTION_JOBFINISHED, nodeid, jobid, res, info))
		return ((CC.RET_OK, self.SVCID, 0), "%s %s" % (nodeid, jobid))
	
	def exportMISSIONNOTIFY(self, channel, nodeid, jobid, params):
		return self.core.missionNotify(channel, nodeid, jobid, params)
	
	def exportCANCELMISSION(self, channel, missionid):
		self.queue.put((ACTION_CANCELMISSION, missionid))
		return (CC.RET_OK, self.SVCID, 0)

# -- DOC --
#
# * mission.unfinished - id
# * mission.queued - id
# * mission.finished - id
#
# * waitQueue - job
# * node.currentJob - job

