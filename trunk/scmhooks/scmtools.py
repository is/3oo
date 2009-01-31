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

# vim: ts=2 expandtab ai sts=2
