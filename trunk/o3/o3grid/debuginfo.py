#
# ==AUTHOR
#   Sin Yu <scaner@gmail.com>
#
# ==MODULE
#   Debug info
#

__all__ = (
	'errorInfo', 'svcInfo', 'returnInfo',)

import constants as CC

errorInfo = {}
svcInfo = {}
returnInfo = {}

def I(d, id):
	d[getattr(CC, id)] = id

def EI(id):
	I(errorInfo, id)

def SI(id):
	I(svcInfo, id)

def RI(id):
	I(returnInfo, id)

# ----
SI('SVC_SYSTEM')
SI('SVC_BASE')

SI('SVC_NAMES')
SI('SVC_HUB')
SI('SVC_AUTOCONFIG')
SI('SVC_SCHEDULE')
SI('SVC_WAREHOUSE')

SI('SVC_WS')
SI('SVC_SPACE')

def SVCIDToStr(svcid):
	if svcInfo.has_key(svcid):
		return svcInfo[svcid][4:]
	else:
		return str(svcid)

# ----
EI('ERROR_UNKNOWN')
EI('ERROR_NO_SERVICE') 
EI('ERROR_NO_FUNCTION')
EI('ERROR_NO_SUCH_OBJECT')

EI('ERROR_SPACE_PUT')
EI('ERROR_SPACE_NO_SUCH_SNIP')
EI('ERROR_SPACE_NO_SUCH_ROOM')
