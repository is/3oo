import threading, Queue
import os, random, time
import struct, zlib
import cPickle as pickle, cStringIO as StringIO
import operator

from o3grid import constants as CC
from o3grid.utility import cout, D as _D, D2 as _D2, DE as _E
from o3grid.protocol import O3Call, O3Channel, O3Space
from o3grid import job

import o3lib.base
from o3lib.fs import StartO3EntityReader

from fastmap import increase as mapincrease, partition as mappartition
from fastmap import fastdumps, fastloads, fastloads3, partitiondumps


MISSIONNAME = "ONEDAY01"
CODEBASE = "oneday01"
MODULENAME = "oneday01.oneday01"
PARTITIONS = 8
MISSIONPREFIX = 'OD01'

# --- Utility for date related
def WeekPostfix(datename):
	dtime = time.strptime(datename, '%Y/%m/%d')
	day = int(time.strftime('%w', dtime))
	week = time.strftime('%W', date)
	if day == 0: day = 7

	tz = time.mktime(dtime)
	begintz = tz - (3600 * 24 * (day - 1))
	endtz = begintz - (3600 * 24 * 6)

	return '%s-%s-%s' % (
		week, 
		time.strftime('%m.%d', time.localtime(begintz)),
		time.strftime('%m.%d', time.localtime(endtz)))
		
# --- OneDay01 Mission Class ---
class MOneDay01(job.Mission):
	def __init__(self, id, kwargs):
		job.Mission.__init__(self, id, kwargs)
		#self.name = MISSIONNAME
		self.codebase = CODEBASE
	
	def setup(self, kwargs):
		job.Mission.setup(self, kwargs)
	
	def prepare(self):
		self.starttime = time.time()
		self.insize0 = 0.0
		self.pv = 0

		res = O3Call(('localhost', CC.DEFAULT_PORT),
			CC.SVC_WAREHOUSE, 'LISTENTITY1', self.kwargs['prefix'])
		entitys = res[2]
		entitys.sort(key = operator.itemgetter('size'), reverse=True)

		res = O3Call(('localhost', CC.DEFAULT_PORT),
			CC.SVC_WAREHOUSE, 'LISTENTITYLOCATION0', [ e['id'] for e in entitys])
		shadows = res[2]

		self.hourres = []
		self.hourinfo = []
		self.partitions = []

		_D('%s:--START--:%s' % (self.id, self.kwargs['prefix']), '|')

		self.totalJob = self.newSJob('C9-SUM', MODULENAME, 'JOneDaySummary')
		self.totalJob.setup0(
			jobname = 'SUM',
			prefix = self.kwargs['prefix'],
			partitions = self.partitions,
			hourinfo = self.hourinfo)
		self.totalJob.fire()

		self.partitionJobs = []
		for i in range(PARTITIONS):
			job = self.newSJob('C1-P%d' % i, MODULENAME, 'JPVPartitionSumJob')
			job.setup0(
				jobname = 'P%d' % i,
				hourres = self.hourres,
				partitionid = i)
			job.fire()
			self.totalJob.need(job)
			self.partitionJobs.append(job)

		serial = 0
		for e in entitys:
			#eid, ename, emtime, esize = e
			eid = e['id']
			ename = e['name']
			emtime = e['mtime']
			esize = e['size']
			sid, snode, saddr, slabel, sname, size = random.choice(shadows[eid])
			taskname = 'C0-%02d' % (serial)
			serial += 1
			job = self.newSJob(taskname, MODULENAME, 'JPVLogHour')
			job.name = job.id
			job.setup({
				'jobname': ename.split('/')[-1].split('.')[0],
				'entityname': ename,
				'entityid': eid,
				'addr': saddr,
				'node': snode,
				'label': slabel,
				'size': esize,})
			job.fire()
			for j in self.partitionJobs:
				j.need(job)

	def jobFinished(self, job, params):
		if job.id.startswith('C0-'):
			self.hourres.append((params['location'], params['resultid']))
			self.insize0 += params.get('insize0', 0.0)
			self.hourinfo.append(params.get('restext'))
		elif job.id.startswith('C1-'):
			self.partitions.append((params['location'], params['resultid']))
		elif job.id.startswith('C9-'):
			cout('-MISSION-END- {%s|%s:%s} %.2fm %.2fs' % (
				self.id, self.name, job.name, self.insize0, time.time() - self.starttime))


# ----- UTILITIES -----
def couttimer(func, *args, **kwargs):
	begin = time.time()
	res = func(*args, **kwargs)
	end = time.time()
	cout('%s - %.2fs' % (func.func_name, end - begin))
	return res
	
# ===
def MapPlusList0(map, l):
	for (k, v) in l.iteritems():
		mapincrease(map, k, v)

# ===
def RemoteReader(queue, node, addr, label, name, size, entityid):
	bs = 512000 * 8
	try:
		S = O3Channel().connect((addr, CC.DEFAULT_PORT))
		res = S(CC.SVC_SPACE, 'ROOMGET1',
			label, name, 0, 0, entityid)

		if res[0] != CC.RET_OK:
			return
		size = res[2]

		rest = size
		while rest != 0:
			buf = S.recvAll(min(bs, rest))
			if not buf:
				break
			rest -= len(buf)
			queue.put(buf)

		S.getMessage()
		S.close()
	finally:
		queue.put(None)
# --end--

def StartRemoteReader(*args):
	thr = threading.Thread(
		name = 'REMOTEREADER',
		target = RemoteReader,
		args = args)
	thr.setDaemon(True)
	thr.start()
	return thr


# ===
class JPVPartitionSumJob(object):
	def __init__(self, params, job):
		self.jobinfo = job
		self.params = params
		self.workspace = job['workspace']

	def run(self):
		params = self.params
		partitionid = params['partitionid']
		ip = {}
		url = {}
		ut = {}
		uc = {}

		for i in self.params['hourres']:
			content = O3Space(i[0]).GET('%s_RES_%d' % (i[1], partitionid))
			(hip, hurl, hut, huc) = fastloads3(content)
			MapPlusList0(ip, hip)
			MapPlusList0(url, hurl)
			MapPlusList0(ut, hut)
			MapPlusList0(uc, huc)

		content = fastdumps((ip, url, ut, uc))

		S = O3Space(('127.0.0.1', CC.DEFAULT_PORT))
		resid = '%s_RES' % self.jobinfo['jobid']
		S.PUT(resid, content)

		self.jobinfo['result'] = {
			'resultid': resid,
			'location': self.workspace.server.entry,
		}

# ===
class JOneDaySummary(object):
	def __init__(self, params, job):
		self.jobinfo = job
		self.params = params
		self.workspace = job['workspace']

	def run(self):
		params = self.params
		ip = {}
		url = {}
		ut = {}
		uc = {}

		for i in self.params['partitions']:
			content = O3Space(i[0]).GET(i[1])
			(hip, hurl, hut, huc) = fastloads(content)

			ip.update(hip)
			url.update(hurl)
			ut.update(hut)
			uc.update(huc)

		cout('%s ip:%d url:%d ut:%d uc:%d' % (
			self.jobinfo['jobid'], len(ip), len(url), len(ut), len(uc)))
		

		O3 = o3lib.base.O3(self.workspace)

		# One Day Result
		prefix = self.params['prefix']
		hourinfo = self.params['hourinfo']

		logname, year, month, day = prefix.split('/')[1:]
		hourname = 'oneday01/%s/day/%s-%s.%s.%s' % (year, logname, year, month, day)

		def hourinfonamekey(x):
			return x[0].split('-')[-1]
		hourinfo.sort(key = hourinfonamekey)
		pv = sum([h[1]['pv'] for h in hourinfo])

		hourtxt = '\n'.join([
			'%s %s' % (hourinfonamekey(h), ','.join([
				'%s:%s' % (i, str(h[1][i])) for i in ('ip', 'ut', 'uc', 'pv', 'url')])
			) for h in hourinfo])
		
		daytxt = '%s/%s.%s.%s - ip:%d,ut:%d,uc:%d,pv:%d,url:%d' % (
			logname, year, month, day, len(ip), len(ut), len(uc), pv, len(url))
		O3.saveResult(hourname, '\n'.join((daytxt, '--hourinfo--', hourtxt)))

		self.jobinfo['result'] = 0
		# --work-point--

# ===
class JPVLogHour(object):
	def __init__(self, params, job):
		self.kwargs = params
		self.jobinfo = job
		self.workspace = job['workspace']
	
	def run(self):
		begin = time.time()
		params = self.kwargs
		entityid = params['entityid']
		entityname = params['entityname']
		addr = params['addr']
		label = params['label']
		size = params['size']
		node = params['node']

		queue = Queue.Queue(10)
		#reader = StartRemoteReader(queue, node, addr, label, entityname, size, entityid)
		reader = StartO3EntityReader(
			queue,
			node = node, addr = addr, label = label,
			name = entityname, size = size, entityid = entityid)

		UL = PVLogCounter0(queue)
		UL.count()
	
		cout('%s ip:%d url:%d ut:%d uc:%d' % (
			self.jobinfo['jobid'],
			len(UL.ip), len(UL.url), len(UL.ut), len(UL.uc)))
	
		# -- Dump dict to string IO buffer
		souts = couttimer(UL.dump, PARTITIONS)

		S = O3Space(('127.0.0.1', CC.DEFAULT_PORT))
		jobid = self.jobinfo['jobid']
		for i in range(PARTITIONS):
			resid = '%s_RES_%d' % (jobid, i)
			S.PUT(resid, souts[i])

		self.jobinfo['result'] = {
			'resultid': jobid,
			'location': self.workspace.server.entry,
			'insize0': UL.bytes / 1024.0 / 1024,
			'restext':[
				jobid, {
					'pv': UL.lines, 'ip': len(UL.ip), 'url': len(UL.url),
					'ut': len(UL.ut), 'uc': len(UL.uc)}
				],
			'debuginfo': '%s at %s - %.2fMb/%.2fs' % (
				jobid,
				self.workspace.server.id,
				UL.bytes / 1024.0/1024,
				time.time() - begin),
		}

# ===
class PVLogCounter0(object):
	def __init__(self, queue):
		self.curline = []
		self.lines = 0
		self.queue = queue
		self.ip = {}
		self.url = {}
		self.ut = {}
		self.uc = {}
		self.bytes = 0
	
	def count(self):
		uc = self.uc
		ut = self.ut
		ip = self.ip
		url = self.url
		queue = self.queue
		lines = 0
		bytes = 0
		pending = ''
		loop = True

		while loop:
			bs = self.queue.get()
			if not bs:
				loop = False
				if pending == '':
					continue
				tokens = pending.split('\n')
				pending = ''
			else:
				bytes += len(bs)
				tokens = bs.split('\n')
				tokens[0] = pending + tokens[0]
				pending = tokens.pop()
			
			for line in tokens:
				try:
					l = line.split('\t')
				
					if l[7][0] == '4':
						continue
		
					mapincrease(ip, l[2])
					mapincrease(url, l[4])
					mapincrease(ut, l[11])
					mapincrease(uc, l[12])
					lines += 1
				except Exception, e:
					_D('EXCEPTION %s %s' % (repr(e), line))
		
		self.lines = lines
		self.bytes = bytes

	# ---
	def dump(self, n):
		#res = []

		ips = partitiondumps(self.ip, n)
		urls = partitiondumps(self.url, n)
		uts = partitiondumps(self.ut, n)
		ucs = partitiondumps(self.uc, n)

		return [ ''.join((ips[x], urls[x], uts[x], ucs[x])) for x in range(n) ]
# --end-- class PVLogCounter01

def generateJob(job, workspace):
	classname = job['class']
	G = globals()
	C = G[classname]

	param = job.get('params', {})
	job['workspace'] = workspace

	return C(param, job)

O3Mission = MOneDay01

