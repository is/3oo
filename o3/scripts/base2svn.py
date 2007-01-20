#!/usr/bin/python

import os

SOURCE='/is/app/o3/base'
TARGET='/is/app/o3/o3svn'

def GetINodeNumber(path1):
	try:
		return os.stat(path1)[1]
	except:
		return -1
	
def IsSameFile(path1, path2):
	return GetINodeNumber(path1) == GetINodeNumber(path2)

def L(str, chr = '|'):
	print '%s %s' % (chr, str)

def ScanDir(source, target, path = ''):
	entries = os.listdir('%s/%s' % (source, path))
	entries.sort()

	for e in entries:
		if e == 'CVS':
			continue
		if e == '.cvsignore':
			continue

		if path == '':
			rpath = e
		else:
			rpath = '/'.join((path, e))

		aspath = '/'.join((source, rpath))
		atpath = '/'.join((target, rpath))

		if os.path.islink(aspath):
			continue
		elif os.path.isfile(aspath):
			if rpath.endswith('.pyc'):
				continue
			if not os.path.exists(atpath):
				os.link(aspath, atpath)
				L('link %s' % rpath)
				continue
			if IsSameFile(aspath, atpath):
				continue
			os.unlink(atpath)
			os.link(aspath, atpath)
			L('update %s' % rpath)
			continue
		elif os.path.isdir(aspath):
			if not os.path.exists(atpath):
				os.mkdir(atpath)
				L('mkdir %s' % rpath)
			ScanDir(source, target, rpath)
			continue

ScanDir(SOURCE, TARGET)
