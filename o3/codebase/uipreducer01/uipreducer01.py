import threading, Queue
import os, random, time
from o3grid import job, constants as CC
from o3grid.protocol import O3Channel, O3Space, O3Call

from o3grid.utility import cout, D as _D, D2 as _D2, DE as _E
from fastmap import inetstr2int as inet2int, _dumps, _loads

import o3lib.base
from o3lib.fs import StartO3EntityReader

VERSION = "0.0.0.1"
CODEBASE = 'uipreducer01'
MODULENAME = 'uipreducer01.uipreducer01'

# ----- utility classes and functions

# === Log Scanner class
# ==C-BaseScanner
class BaseScanner(object):
	def __init__(self, queue):
		self.queue = queue
		self.lines = 0
		self.bytes = 0
	
	# ---
	# Core Loop for log scanner
	# ---
	def scan(self): # ==M-BaseScanner-scan
		loop = True
		pending = ''
		queue = self.queue

		while loop:
			block = queue.get()
			if not block:
				loop = False
				if pending == '':
					continue
				lines = pending.split('\n')
				self.lines += len(lines)
				pending == ''
			else:
				lines = block.split('\n')
				lines[0] = pending + lines[0]
				pending = lines.pop()
				self.lines += len(lines)
				self.bytes += len(block)
			self.analyse(lines)
	
# === Union Log IP counter
# ==C-UnionLogIPCounter
class UnionLogIPCounter(object):
	def __init__(self):
		self.uume = set()
		self.itv = set()
		self.dzh = set()
		self.tt = set()

		self.dzhDomains = (
			'dzh', 'dzh2', 'dzh1',
			'search', 'search2', 
			'txt', 'topic', 'best')
		self.ttDomains = ('tt', 'post')


	# ==M-UnionLogIPcounter-analyse
	def analyse(self, lines): 
		dzhdomains = self.dzhDomains
		ttdomains = self.ttDomains

		for line in lines:
			head, s, next = line.partition('|rlink|')
			if not s:
				continue
			
			if next[2] == 'u': # UUME
				self.uume.add(inet2int(head.split(' ', 1)[0]))
				continue
			if next[2] == 'i': # ITV
				self.itv.add(inet2int(head.split(' ', 1)[0]))
				continue

			# MOP
			domain = next[13:].partition('.mop.com/')[0]
			if domain in dzhdomains:
				self.dzh.add(inet2int(head.split(' ', 1)[0]))
				continue

			if domain in ttdomains:
				self.tt.add(inet2int(head.split(' ', 1)[0]))
				continue

# ==C-S15LogIPCounter
class S15LogIPCounter(object):
	def __init__(self):
		self.ip = set()
	
	# ==M-S15LogIPCounter-analyse
	def analyse(self, lines):
		for line in lines:
			tokens = line.split('\t')
			if tokens[6] == '404':
				continue
			try:
				self.ip.add(inet2int(tokens[2]))
			except Exception, e:
				print tokens
				print line
				raise e
		
# ==F-StartRemoteLogScanner
def StartRemoteLogScanner(cType, **params):
	class Scanner(BaseScanner, cType):
		def __init__(self, queue):
			BaseScanner.__init__(self, queue)
			cType.__init__(self)
	
	queue = Queue.Queue(10)
	reader = StartO3EntityReader(queue, **params)
	scanner = Scanner(queue)
	return (queue, reader, scanner)
	

# ----- job classes
# ==C-JOBBase
class JOBBase(object):
	def __init__(self, params, job):
		self.info = job
		self.workspace = job['workspace']
		self.params = params
	
	# ==M-JOBBase-setupResult1
	def setupResult1(self, begin):
		self.info['result'] = {
			'resultid': self.info['jobid'],
			'location': self.workspace.server.entry,
			'debuginfo': '%s at %s - %.2fs' % (
				'-'.join(self.info['jobid'].split('-', 2)[:2]),
				self.workspace.server.id,
				time.time() - begin,
			)
		}

	# ==M-JOBBase-updateResult
	def updateResult(self, *args, **kwargs):
		self.info['result'].update(*args, **kwargs)

	# ==M-JOBBase-setupResult0
	def setupResult0(self, begin, scanner):
		self.info['result'] = {
			'resultid': self.info['jobid'],
			'location': self.workspace.server.entry,
			'insize0': scanner.bytes / 1024.0 / 1024,
			'debuginfo': '%s at %s - %.2fMb/%.2fs' % (
				'-'.join(self.info['jobid'].split('-', 2)[:2]),
				self.workspace.server.id,
				scanner.bytes / 1024.0 / 1024,
				time.time() - begin),
		}

# ==C-JOBS15IPHour
class JOBS15IPHour(JOBBase):
	# - addr, label, entityname, eneityid, size
	# - hour, logname
	# ==M-JOBS15IPHour-run
	def run(self):
		P = self.params
		begin = time.time()
		#eid = params['entityid']
		#ename = params['entityname']
		#addr = params['addr']
		#label = params['label']
		#node = params['node']
		(queue, reader, scanner) = StartRemoteLogScanner(S15LogIPCounter, 
			node = P['node'],
			addr = P['addr'], 
			label = P['label'],
			name = P['entityname'], 
			#size = P['size'],
			size = 0,
			entityid = P['entityid'])
		scanner.scan()
		S = O3Space()
		name = self.info['jobid']
		S.PUT(name, _dumps(scanner.ip))
		self.setupResult0(begin, scanner)
		cout('-JOB-OUT- %s %d' % (
			self.info['jobid'],
			len(scanner.ip)))
			

# ==C-JOBUnionIPHour
class JOBUnionIPHour(JOBBase):
	# ==M-JOBUnionIPHour-run
	def run(self):
		P = self.params
		begin = time.time()
		#eid = params['entityid']
		#ename = params['entityname']
		#addr = params['addr']
		#label = params['label']
		#node = params['node']
		(queue, reader, scanner) = StartRemoteLogScanner(UnionLogIPCounter,
			node = P['node'],
			addr = P['addr'], 
			label = P['label'],
			name = P['entityname'], 
			size = 0,
			#size = P['size'],
			entityid = P['entityid'])
		scanner.scan()
		S = O3Space()
		name = "%s-" % (self.info['jobid'])
		S.PUT(name + "dzh", _dumps(scanner.dzh))
		S.PUT(name + "tt", _dumps(scanner.tt))
		S.PUT(name + "uume", _dumps(scanner.uume))
		S.PUT(name + "itv", _dumps(scanner.itv))

		cout('-JOB-OUT- %s uume:%d dzh:%d tt:%d itv:%d' % (
			self.info['jobid'], 
			len(scanner.uume), len(scanner.dzh), 
			len(scanner.tt), len(scanner.itv)))
		self.setupResult0(begin, scanner)

# ==C-JOBUnionIPDay
class JOBUnionIPDay(JOBBase):
	def run(self):
		begin = time.time()
		P = self.params
		logname = P['logname']
		ip = set()

		for i in P['hours']:
			content = O3Space(i[0]).GET('%s-%s' % (i[1], logname))
			hourip = _loads(content)
			ip.update(hourip)

		name = "%s" % (self.info['jobid'])
		S = O3Space()
		S.PUT(name, _dumps(ip))
		self.setupResult1(begin)
		self.updateResult(logname = logname)

		cout('-JOB-OUT- %s %d' % (
			self.info['jobid'], len(ip)))
		
# ==C-JOBS15IPDay
class JOBS15IPDay(JOBBase):
	def run(self):
		begin = time.time()
		P = self.params
		logname = P['logname']
		ip = set()

		for i in P['hours']:
			content = O3Space(i[0]).GET(i[1])
			ip.update(_loads(content))
		name = '%s' % (self.info['jobid'])
		O3Space().PUT(name, _dumps(ip))

		self.setupResult1(begin)
		self.updateResult(logname = logname)
		cout('-JOB-OUT- %s %d' % (
			self.info['jobid'], len(ip)))
		
# ==C-JOBIPDayAll
class JOBIPDayAll(JOBBase):
	def run(self):
		begin = time.time()
		P = self.params
		lognames = P['lognames']
		logfiles = P['logfiles']

		restext = []
		for logname in lognames:
			f1 = logfiles[logname]
			f2 = logfiles["union-" + logname]
			c1 = O3Space(f1[0]).GET(f1[1])
			c2 = O3Space(f2[0]).GET(f2[1])
			ip1 = _loads(c1)
			ip2 = _loads(c2)
			cout('%s: ORIGIN:%d UNION:%d ORIGIN-UNION:%d/%d' % (
				logname, len(ip1), len(ip2), len(ip1 - ip2), len(ip1) - len(ip2)))
			restext.append('%s: ORIGIN:%d UNION:%d ORIGIN-UNION:%d/%d' % (
				logname, len(ip1), len(ip2), len(ip1 - ip2), len(ip1) - len(ip2)))
			cout('-JOB-OUT-RESULT %s %s: ORIGIN:%d UNION:%d ORIGIN-UNION:%d/%d' % (
				P['logdate'], logname, len(ip1), len(ip2), len(ip1 - ip2), len(ip1) - len(ip2)))
		
		restext.sort()

		year,sep,date = P['logdate'].partition('.')
		resname = 'uip01/%s/%s' % (year, date)
		O3 = o3lib.base.O3(self.workspace)
		O3.saveResult(resname, '\n'.join(restext))
		self.info['result'] = 0
	
# ----- mission control class
# ==F-EntityNameToDate
def EntityNameToHour(name): 
	if name.endswith('.iz0'):
		return '.'.join(name[:-4].split('/')[-4:])
	return '.'.join(name.split('/')[-4:])

def EntityNameToDate(name): return '.'.join(name.split('/')[-4:-1])
def EntityNameToLogName(name): return name.split('/')[1]

def GetEntities(prefix):
	res = O3Call(('localhost', CC.DEFAULT_PORT),
		CC.SVC_WAREHOUSE, 'LISTENTITY0', prefix)
	if res[0] != CC.RET_OK:
		return None
	entities = res[2]
	
	if not len(entities):
		return None
	
	res = O3Call(('localhost', CC.DEFAULT_PORT),
		CC.SVC_WAREHOUSE, 'LISTENTITYLOCATION0', [e[0] for e in entities])
	shadows = res[2]

	res = []
	for e in entities:
		s = random.choice(shadows[e[0]])
		e.append(s)

	return entities
	
# ==C-MissionIPReducer
class MissionIPReducer(job.Mission):
	def __init__(self, id, kwargs = None):
		job.Mission.__init__(self, id, kwargs)
		self.name = 'IPReduce'
		self.codebase = CODEBASE

	def setup(self, kwargs):
		self.kwargs = kwargs

	def start(self):
		_D('%s:--IPReducer--%s--' % (self.id, self.kwargs['date']))
		self.starttime = time.time()
		cout('-JOB-OUT- START--%s' % (self.id))

		date = self.kwargs['date']
		datename = date.replace('/', '.')

		lognames = self.kwargs['lognames']

		self.logfiles = dict()
		lastJob = self.newSJob('Z0-%s' % datename , MODULENAME, 'JOBIPDayAll')
		lastJob.setup0(
			lognames = lognames,
			logfiles = self.logfiles,
			logdate = datename,
		)

		dayJobs = []
		hourJobs = {}
		self.hourIPS = {}
		for l in lognames:
			self.hourIPS[l] = list()
			hourJobs[l] = list()
			job = self.newSJob('D2-%s-%s' % (l, datename), MODULENAME, 'JOBS15IPDay')
			job.setup0(
				hours = self.hourIPS[l],
				logname = l,
				date = date)

			hourLogs = GetEntities('plog/%s/%s' % (l, date))
			serial = 0
			for e in hourLogs:
				eid, ename, emtime, esize = e[:4]
				sid, snode, saddr, slabel, sname, size = e[-1]
				hJob = self.newSJob('H2-%02d-%s-%s' % (serial, l, EntityNameToHour(ename)),
					MODULENAME, 'JOBS15IPHour')
				hJob.name = hJob.id
				hJob.setup0(
					node = snode,
					entityname = ename,
					entityid = eid,
					addr = saddr,
					label = slabel,
					size = esize,
					logname = l,
					logdate = EntityNameToDate(ename),
					loghour = EntityNameToHour(ename),
				)
				hourJobs[l].append(hJob)
				job.need(hJob)
				serial += 1

			dayJobs.append(job)
			lastJob.need(job)

		unionDayJobs = []
		self.unionHour = []
		for l in lognames:
			job = self.newSJob('D1-%s-%s' % (l, datename), MODULENAME, 'JOBUnionIPDay')
			job.setup0(logname = l, hours = self.unionHour)
			unionDayJobs.append(job)
			lastJob.need(job)

		unionHourJobs = []
		unionLogs = GetEntities('plog/mopunion/%s/' % date.replace('.', '/'))
		serial = 0
		for e in unionLogs:
			eid, ename, emtime, esize = e[:4]
			sid, snode, saddr, slabel, sname, size = e[-1]
			job = self.newSJob('H1-%02d-%s' % (serial, EntityNameToHour(ename)),
				MODULENAME, 'JOBUnionIPHour')
			job.name = job.id
			job.setup0(
				node = snode,
				entityname = ename,
				entityid = eid,
				addr = saddr,
				label = slabel,
				size = esize,
				logname = 'mopunion',
				logdate = EntityNameToDate(ename),
				loghour = EntityNameToHour(ename),
			)
			unionHourJobs.append(job)
			for j in unionDayJobs:
				j.need(job)
			serial += 1

		for j in unionHourJobs: j.fire()
		for j in unionDayJobs: j.fire()
		for j in dayJobs: j.fire()
		for js in hourJobs.values():
			for j in js: j.fire()
		lastJob.fire()

		self.insize0 = 0
	
	def jobFinished(self, job, params):
		if params == None:
			cout('%s - failed' % job.id)
		P = params

		if job.id.startswith('D2-'):
			logname = job.id.split('-')[1]
			self.logfiles[logname] = (P['location'], P['resultid'])
		if job.id.startswith('D1-'):
			logname = job.id.split('-')[1]
			self.logfiles['union-%s' % logname] = (P['location'], P['resultid'])
		if job.id.startswith('H2-'):
			logname = job.id.split('-')[2]
			self.hourIPS[logname].append((P['location'], P['resultid']))
			self.insize0 += P['insize0']
		if job.id.startswith('H1-'):
			self.unionHour.append((P['location'], P['resultid']))
			self.insize0 += P['insize0']
		if job.id.startswith('Z0-'):
			cout('-MISSION-FINISHED- %.2fM in %.2fs' % (self.insize0, time.time() - self.starttime))

def generateJob(job, workspace):
	classname = job['class']
	G = globals()
	C = G[classname]

	param = job.get('params', {})
	job['workspace'] = workspace
	return C(param, job)
# ----- other instruction

# ----- classes and functions for test
# ==C-LocalLogScanner
class LocalScanner(object):
	def __init__(self, filename):
		self.fn = filename
		self.lines = 0
		self.bytes = 0

	# ==M-LocalScanner-scan
	def scan(self): 
		loop = True
		pending = ''
		bs = 1024 * 1023 * 8
		fin  = file(self.fn)

		while loop:
			block = fin.read(bs)
			if not block:
				loop = False
				if not pending:
					continue
				lines = pending.split('\n')
				self.lines += len(lines)
				pending = '' 
			else:
				lines = block.split('\n')
				lines[0] = pending + lines[0]
				pending = lines.pop()
				self.lines += len(lines)
				self.bytes += len(block)
	
			self.analyse(lines)
		fin.close()

# ---
# ==F-testLocalUnionLogScanner
def testLocalUnionLogScanner(): 
	class LocalUnionLogScanner(LocalScanner, UnionLogIPCounter):
		def __init__(self, fn):
			LocalScanner.__init__(self, fn)
			UnionLogIPCounter.__init__(self)

	u = LocalUnionLogScanner('/tmp/access.union')
	u.scan()
	print 'uume:%d dzh:%d tt:%d' % (len(u.uume), len(u.dzh), len(u.tt))

# ==F-testLocalS15LogScanner
def testLocalS15LogScanner():  
	class LocalS15LogScanner(LocalScanner, S15LogIPCounter):
		def __init__(self, fn):
			LocalScanner.__init__(self, fn)
			S15LogIPCounter.__init__(self)
	
	u = LocalS15LogScanner('/tmp/0200_')
	u.scan()
	print 'ips:%d' % len(u.ip)

# --- main ---
if __name__ == '__main__':
	#testLocalUnionLogScanner()
	testLocalS15LogScanner()
