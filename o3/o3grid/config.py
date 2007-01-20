#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Config
#

import os

def GetConfigCode(name):
	paths = [
		'%s/%s' % ('/is/app/o3/etc', name),
		name ]
	for fn in paths:
		if os.path.isfile(fn):
			break
	
	fin = file(fn, 'r')
	configcode = fin.read()
	fin.close()

	return configcode

def Load(name):
	configcode = GetConfigCode(name)
	exec configcode
	return _C
