#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Mission and Job
#

import threading, time

import constants as CC

class MissionBase(object): pass
class SJobBase(object): pass

# ------
# mid - Mission id
# jid - Job ID
# jobid - Full job ID
# mname - Mission name
# jname - Job name
# ------
class Mission(MissionBase):
	def __init__(self, id, kwargs = None):
		self.serial = 0
		self.id = id

		self.jobs = {}
		self.queued = {}
		self.unfinished = {}

		self.name = 'NoNameMission'
		self.lock = threading.Lock()

		self.kwargs = None
		self.schedule = None
		self.codebase = None
	
	# ---
	def newSJob(self, id, modulename, classname):
		sjob = SJob(self, id)
		sjob.modulename = modulename
		sjob.classname = classname

		self.jobs[id] = sjob
		self.unfinished[id] = sjob
		return sjob

	def setup(self, kwargs):
		self.name = kwargs.get('name', self.name)
		self.kwargs = kwargs

	def start(self): pass
	def finished(self): pass
	def jobFinished(self, job, params): pass
	def notify(self, channel, node, job, params):
		return (CC.RET_OK, CC.SVC_SCHEDULE, 0)
	
class SJob(SJobBase):
	def __init__(self, mission, id):
		self.id = id
		self.mission = mission
		self.codebase = mission.codebase
		self.jobid = '%s:%s' % (mission.id, id)
		self.prevReady = []
		self.prev = []
		self.next = []
		self.inResource = []
		self.outResource = []
		self.attrs = {}
		self.params = None
		self.name = 'NoNameJob'
		self.runat = None
		self.modulename = None
		self.classname = None

	def need(self, job):
		self.prev.append(job)
		job.next.append(self)

	def fire(self):
		self.createtime = time.time()
		self.mission.jobs[self.id] = self
		self.mission.unfinished[self.id] = self

	def setup0(self, **kwargs):
		self.setup(kwargs)
	
	def setup(self, kwargs):
		self.params = kwargs
		self.name = kwargs.get('jobname', self.name)

	def getJobParams(self):
		job = {}

		job['codebase'] = self.codebase
		job['module'] = self.modulename
		job['class'] = self.classname
		job['jobid'] = self.jobid
		job['jid'] = self.id
		job['jname'] = self.name
		job['mname'] = self.mission.name
		job['params'] = self.params

		return job
