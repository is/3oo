"""
Basic functions and utilities
"""

class RepoConfig(object):
  def __init__(self):
    self.repos = {}
    self.defaults = {}
    self.autoUpdatePath = True

  def get3(self, repo, path, opt): 
    if not (repo, opt) in self.repos:
      return self.defaults.get(opt)

    keys, map = self.repos[repo, opt]
    for k in keys:
      if path.startswith(k):
        return map[k]

    return self.defaults.get(opt)

  def set3(self, repo, path, opt, value):
    if not (repo, opt) in self.repos:
      repocfg = {}
      repocfg[path] = value
      v = [None, repocfg]
      self.repos[repo, opt] = v
    else:
      v = self.repos[repo, opt]
      v[1][path] = value

    if self.autoUpdatePath:
      self.updatePath(v)

  def setAutoUpdatePath(self, v):
    self.autoUpdatePath = v

  def updatePaths(self):
    for v in self.repos.values():
      self.updatePath(v)

  def updatePath(self, v):
    keys = v[1].keys()
    keys.sort(reverse = True)
    v[0] = keys

  def setDefault(self, opt, value):
    self.defaults[opt] = value
# ---- end of RepoConfig

def LoadRepoConfig(fn):
  cf = RepoConfig()
  m = __import__(fn)
  m.setup(cf)
  return cf
# -- end


def FileExtMatch(pattern, ext):
  if pattern == None or pattern == "":
    return True

  tokens = pattern.split(',')

  for token in tokens:
    if token == '+':
      return True
    elif token == '-':
      return False

    sign = '+'
    if token[0] in ('+', '-'):
      sign = token[0]
      token = token[1:]

    if ext == token:
      if sign == '+':
        return True
      else:
        return False
  return False
# --end--

def VersionString(l):
  return '.'.join(['%s' % x for x in l])
# --end--

def FileExt(fn):
  p1, p2, p3 = fn.rpartition('.')
  if not p2:
    return ''

  if p3.find('/') != -1:
    return ''
  return p3.lower()
# --end--


# vim: ts=2 expandtab ai sts=2
