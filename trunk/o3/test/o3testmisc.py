import os

HOME = os.environ.get('HOME', '/root')
O3PROFILEDIR = HOME + '/.o3'

def IsDebugMission(missionname):
	m1 = O3PROFILEDIR + '/_debug/all'
	m2 = O3PROFILEDIR + '/_debug/' + missionname
	if os.path.exists(m1):
		return True
	if os.path.exists(m2):
		return True
	return False
