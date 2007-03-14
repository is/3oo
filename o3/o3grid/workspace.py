#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   WorkSpace
#

WORKSPACE_VERSION = '0.0.2.9'
# -----
import threading
import os, sys
from cStringIO import StringIO
from traceback import print_tb, print_stack

from service import ServiceBase
from protocol import CreateMessage, CreateMessage0, \
  GetMessageFromSocket, O3Channel, O3Call
from utility import mkdir_p, D as _D, D2 as _D2, DE as _E
import constants as CC

class WorkSpaceService(ServiceBase):
	SVCID = CC.SVC_WORKSPACE
	svcDescription = "Workspace Service"
	svcName = 'WORKSPACE'
	svcVersion = WORKSPACE_VERSION

	ADVERT_INTERVAL = 5
	def __init__(self, server):
		self.server = server
		self.lock = threading.Lock()
		self.codebase = {}
		self.jobs = {}
		self.local = threading.local()
	
	def setup(self, conf):
		cf = conf['workspace']
		self.base = cf['base']
		self.tag = cf.get('tag', 'normal')
		sys.path.append(self.base)

	def fillScriptFile(self, fn, contents):
		path = '%s/%s' % (self.base, fn)
		mkdir_p(os.path.dirname(path))
		fout = file(path, 'w')
		fout.write(contents)
		fout.close()

	def loadCodeBase(self, name, version):
		if self.codebase.has_key(name):
			return True

		svcpoint = self.server.resolv('HUB')
		channel = O3Channel()
		try:
			channel.connect(svcpoint)
	
			res = channel(CC.SVC_HUB, 'GETCODEBASE', name, version)
			if res[0] == CC.RET_ERROR:
				self.local.lastError = res
				return False
	
			codebase = res[2]
	
			for fn in codebase['files']:
				res = channel(CC.SVC_HUB, 'GETSCRIPTFILE', fn)
				if res[0] == CC.RET_ERROR:
					self.local.lastError = res
					return False
				self.fillScriptFile(fn, res[4])
	
			self.codebase[name] = codebase
			return True
		finally:
			channel.close()

	def activate(self):
		svcids = self.server.svc.keys()

		# Register workspace on schedule
		schedule = self.server.resolv('SCHEDULE')
		entry = self.server.entry
		nodeid = self.server.id
		starttime = self.server.starttime
		self.server.addTimer2('workspace_advert', self.ADVERT_INTERVAL, True, 
			self.advert, args = (schedule, nodeid, entry, starttime))

		self.schedule = schedule
		self.nodeid = nodeid
		self.entry = entry
			
	# ---
	def advert(self, schedule, nodeid, entry, starttime):
		self.lock.acquire()
		jobs = self.jobs.keys()
		self.lock.release()

		channel = O3Channel()
		channel.connect(schedule)
		channel(CC.SVC_SCHEDULE, "WORKSPACEADVERT", 
			nodeid, entry, self.tag, starttime, jobs)
		channel.close()

	# ---
	def unloadCodeBase(self, name):
		if not self.codebase.has_key(name):
			return False

		codebase = self.codebase[name]
		del self.codebase[name]

		for mn in codebase['modules']:
			try:
				del sys.modules[mn]
			except KeyError:
				pass

		return True

	def exportLOADCODEBASE(self, channel, name, version):
		if self.loadCodeBase(name, version):
			return (CC.RET_OK, self.SVCID, 0)
		else:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_UNKNOWN)
		
	def exportUNLOADCODEBASE(self, channel, name):
		if self.unloadCodeBase(name):
			return (CC.RET_OK, self.SVCID, name)
		else:
			return (CC.RET_ERROR, self.SVCID, CC.ERROR_UNKNOWN)
	
	def exportUNLOADO3LIB(self, channel):
		deleted = [ x for x in sys.modules.keys() if x.startswith('o3lib') ]
		deleted.sort(reverse = False)
		for x in deleted:
			del sys.modules[x]
		return (CC.RET_OK, self.SVCID, deleted)

	def exportSTARTJOB(self, channel, job):
		codebasenames = job['codebase']
		if type(codebasenames) == str:
			codebasenames = [codebasenames, ]
		#codebasename = job['codebase']
		for codebasename in codebasenames:
			if not self.loadCodeBase(codebasename, None):
				return (CC.RET_ERROR, 
					self.SVCID, self.local.lastError[2], self.local.lastError)

		thr = threading.Thread(
			name = 'WORKSPACE-RUNNER', 
			target = self.executeJob,
			args = (channel, job))
		thr.setDaemon(1)
		thr.start()
		return ((CC.RET_OK, self.SVCID, job.get('jobid')),
			job.get('jobid'))

	# ---
	def jobStartup(self, job):
		self.lock.acquire()
		try:
			self.jobs[job['jobid']] = job
		finally:
			self.lock.release()

	def jobFinished(self, job):
		self.lock.acquire()
		try:
			del self.jobs[job['jobid']]
		finally:
			self.lock.release()

	def executeJob(self, channel, job):
		"""JOB Threading"""
		#job["threading"] = threading.currentThread()
		self.jobStartup(job)

		modulename = job['module']
		__import__(modulename)

		runtimeException = None

		try:
			bootmod = sys.modules[modulename]
			J = bootmod.generateJob(job, self)
			J.run()
		except Exception, e:
			runtimeException = e

			reName = e.__class__.__name__
			reRepr = repr(e)
			reStr = str(e)

			s = StringIO()
			print_tb(sys.exc_info()[2], limit = 10, file = s)
			reTraceback = s.getvalue()

			_E(e)

		self.jobFinished(job)
		info = job.get('info', {})
		if runtimeException:
			s = StringIO()
			print_tb
			info['exception'] = {
				'typename': reName,
				'repr': reRepr,
				'str': reStr,
				'traceback': reTraceback,
			}

		channel = O3Channel()
		channel.connect(self.schedule)
		channel(
			CC.SVC_SCHEDULE, 'JOBFINISHED', 
			self.nodeid, 
			job.get('jobid'), 
			job.get('result', None), info)
		channel.close()
