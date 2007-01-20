#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Utility
#

import os, time, sys
import socket

from cStringIO import StringIO
from traceback import print_tb, print_stack

def mkdir_p(dir):
	if os.path.isdir(dir):
		pass
	
	elif os.path.isfile(dir):
		raise OSError("a file with same name as the desired " \
			"dir, '%s', already exists." % newdir)
	else:
		head, tail = os.path.split(dir)

		if head and not os.path.isdir(head):
			mkdir_p(head)
		if tail:
			os.mkdir(dir)

def PrepareDir(path):
	mkdir_p(os.path.dirname(path))

# ===
dinfo = {}
def LogSetup(di):
	dinfo.update(di)

def D2(str, chr = '='):
	s = dinfo.get('socket', None)
	if s:
		try:
			s.send("%s %c %s" % (dinfo['hostname'], chr, str))
		except socket.error:
			pass
			


def D(str, chr = '='):
	print '%s %c %s' % (time.strftime('%H:%M:%S'), chr, str)
	s = dinfo.get('socket', None)
	if s:
		try:
			s.send("%s %c %s" % (dinfo['hostname'], chr, str))
		except socket.error:
			pass

def cout(str):
	str = str.replace('\n', '         | ')
	D(str, '|')

def DE(e):
	D('--%s--' % repr(e), 'E')
	D('{{%s}}' % str(e), 'E')
	s = StringIO()
	print_tb(sys.exc_info()[2], limit = 4, file = s)
	for txt in s.getvalue().split('\n'):
		D(txt, '|')

# ====
def appendinmap(dict, key, value):
	l = dict.get(key, None)
	if l != None:
		l.append(value)
	else:

		dict[key] = [value,]

def removeinmap(dict, key, value):
	l = dict.get(key)
	l.remove(value)
	if len(l) == 0:
		del dict[key]

def leninmap(dict, key):
	return len(dict.get(key, ''))

# ---
def sizeK(size):
	return (size + 1023) / 1024

# ---
def RoomLocation(node, entry, label, path): # EXPORT-FUNCTION
	if path.startswith('/'):
		path = path.strip('/')
	
	return '%s:%s,%d:_%s/%s' % (node,
		entry[0], entry[1], label, path)

def RoomLocationToTuple(location): # EXPORT-FUNCTION
	(node, entrystr, fullpath) = location.split(':', 2)
	addr, port = entrystr.split(',')
	entry = (addr, int(port))
	label, path = fullpath.split('/', 1)
	label = label[1:]
	return (node, entry, label, path)


# ---
class LogFileSpliter(object):
	def __init__(self, filename, blocksize, etc = 1.05):
		self.fn = filename
		self.etc = etc
		self.bs = blocksize
		self.res = []
	
	def getFileSize(self):
		return os.stat(self.fn)[6]	
	
	def splitAtLineEnd(self):
		off = 0
		bs = self.bs
		maxbs = int(bs * self.etc)

		size = self.getFileSize()
		self.size = size

		fin = file(self.fn, 'r')
		try:
			while True:
				if size < off + maxbs:
					return len(self.res)

				fin.seek(off + bs)
				buffer = fin.read(4096)
				padding = buffer.find('\n')
				off += bs + padding + 1
				self.res.append(off)
		finally:
			fin.close()
	split = splitAtLineEnd

# ===== Simple File Logger
class FileLogger(object):
	def __init__(self, fn):
		self.fn = fn
		self.fout = file(fn, 'a+')
	
	def L(self, str):
		self.fout.write('%s %s\n' % (time.strftime('%m%d_%H:%M:%S'), str))
		self.fout.flush()

	def close(self):
		self.fout.close()
