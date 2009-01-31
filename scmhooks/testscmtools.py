import os, sys, unittest, inspect
from scmtools import RepoConfig

class RepoConfigBaseTests(unittest.TestCase):
  # --
  def setUp(self):
    pass
  
  def testSetDefault(self):
    repo = RepoConfig()

    repo.setDefault('encoding', 'gbk')
    self.assert_(repo.defaults.get('encoding') == 'gbk')
    repo.setDefault('encoding', 'utf8')
    self.assert_(repo.defaults.get('encoding') == 'utf8')
    self.assert_(repo.defaults.get('otherthings') == None)
    repo.setDefault('en', 'gbk')

  def testSet3(self):
    repo = RepoConfig()
    repo.setAutoUpdatePath(False)

    r0 = '/R0'
    r1 = '/R1'

    p0 = 'Path0/'
    p1 = 'Path1/'
    p2 = 'Path2/'

    o0 = 'encoding'

    rs = repo.repos
    repo.set3(r0, p0, o0, 'gbk')
    assert rs[r0, o0][1][p0] == 'gbk'

    repo.set3(r0, p1, o0, 'utf8')
    assert not rs[r0, o0][1][p0] == 'utf8'
    assert rs[r0, o0][1][p1] == 'utf8'

  def testUpdatePath(self):
    repo = RepoConfig()
    repo.setAutoUpdatePath(False)
    
    r0 = "/R0"
    opt = "encoding"

    repo.set3(r0, 'abc/', opt, 'utf8')
    repo.set3(r0, 'abcdef/', opt, 'utf8')
    repo.set3(r0, 'abc/def/', opt, 'utf8')
    repo.set3(r0, '', opt, 'gbk')
    repo.updatePaths()
    v = repo.repos.get((r0, opt))
    assert v[0] == ['abcdef/', 'abc/def/', 'abc/', '']

  def testAutoUpdatePath(self):
    repo = RepoConfig()
    r0 = "/R0"
    opt = "encoding"

    repo.set3(r0, 'abc/', opt, 'utf8')
    repo.set3(r0, 'abcdef/', opt, 'utf8')
    repo.set3(r0, 'abc/def/', opt, 'utf8')
    repo.set3(r0, '', opt, 'gbk')
    v = repo.repos.get((r0, opt))
    assert v[0] == ['abcdef/', 'abc/def/', 'abc/', '']

  def testGet3(self):
    repo = RepoConfig()

    r0 = '/R0'
    r1 = '/R1'
    r2 = '/R2'
    opt = 'encoding'
    opt2 = 'encoding2'

    repo.setDefault(opt, 'v0')
    repo.set3(r0, 'abc/', opt, 'v1')
    repo.set3(r0, 'abcdef/', opt, 'v2')
    repo.set3(r0, 'abc/def/', opt, 'v3')
    repo.set3(r1, '', opt, 'v4')

    assert repo.get3(r0, '', opt2) == None
    assert repo.get3(r2, 'abc/', opt) == 'v0'
    assert repo.get3(r1, 'abc/', opt) == 'v4'
    assert repo.get3(r0, 'abc/def', opt) == 'v1'
    assert repo.get3(r0, 'abc/def/abc', opt) == 'v3'
    assert repo.get3(r0, 'abcdef/abc', opt) == 'v2'
    assert repo.get3(r0, 'def/', opt) == 'v0'
    assert repo.get3(r0, 'abcdef/abc', opt2) == None
    assert repo.get3(r2, 'abc/def', opt2) == None
# ---- end of RepoConfigBaseTests 

    
if __name__ == '__main__':
  unittest.main()
# vim: ts=2 sts=2 expandtab ai