import os, sys, unittest, inspect
from scmtools import RepoConfig, LoadRepoConfig
from scmtools import VersionString, FileExtMatch
from scmtools import FileExt

class SCMToolsTests(unittest.TestCase):
  def testFileExtMatch(self):
    assert FileExtMatch(None, 'java')
    assert FileExtMatch('', 'java')
    assert FileExtMatch('+', 'java')

    assert not FileExtMatch('-', 'java')

    assert FileExtMatch('java', 'java')
    assert FileExtMatch('+java', 'java')
    assert not FileExtMatch('+java', 'c')
    assert not FileExtMatch('-java', 'java')

    assert FileExtMatch('java,c', 'java')
    assert FileExtMatch('java,c', 'c')
    assert FileExtMatch('-java,+', 'c')
    assert not FileExtMatch('java,c', 'cpp')
    assert not FileExtMatch('-java,c', 'java')
    assert not FileExtMatch('java,-', 'c')
    assert FileExtMatch('java,c,cpp,txt,-', 'txt')
    assert not FileExtMatch('java,c,cpp,-txt,+', 'txt')

  def testVersionString(self):
    assert VersionString((1,2,3)) == '1.2.3'
    assert VersionString((1,)) == '1'
    assert VersionString((1,2,'dev','pre5')) == '1.2.dev.pre5'
    assert VersionString(['dev', 2]) == 'dev.2'
  # --end--

  def testFileExt(self):
    assert FileExt('abc.jpg') == 'jpg'
    assert FileExt('abcdef') == ''
    assert FileExt('hello.world/abc') == ''
    assert FileExt('abc.Def') == 'def'
  # --end--
# --cend--


class RepoConfigBaseTests(unittest.TestCase):
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
    repo.set3(o0, r0, p0, 'gbk')
    assert rs[r0, o0][1][p0] == 'gbk'

    repo.set3(o0, r0, p1, 'utf8')
    assert not rs[r0, o0][1][p0] == 'utf8'
    assert rs[r0, o0][1][p1] == 'utf8'

  def testUpdatePath(self):
    repo = RepoConfig()
    repo.setAutoUpdatePath(False)
    
    r0 = "/R0"
    opt = "encoding"

    repo.set3(opt, r0, 'abc/', 'utf8')
    repo.set3(opt, r0, 'abcdef/', 'utf8')
    repo.set3(opt, r0, 'abc/def/', 'utf8')
    repo.set3(opt, r0, '', 'gbk')
    repo.updatePaths()
    v = repo.repos.get((r0, opt))
    assert v[0] == ['abcdef/', 'abc/def/', 'abc/', '']

  def testAutoUpdatePath(self):
    repo = RepoConfig()
    r0 = "/R0"
    opt = "encoding"

    repo.set3(opt, r0, 'abc/', 'utf8')
    repo.set3(opt, r0, 'abcdef/', 'utf8')
    repo.set3(opt, r0, 'abc/def/', 'utf8')
    repo.set3(opt, r0, '', 'gbk')
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
    repo.set3(opt, r0, 'abc/', 'v1')
    repo.set3(opt, r0, 'abcdef/', 'v2')
    repo.set3(opt, r0, 'abc/def/', 'v3')
    repo.set3(opt, r1, '', 'v4')

    assert repo.get3(opt2, r0, '') == None
    assert repo.get3(opt, r2, 'abc/') == 'v0'
    assert repo.get3(opt, r1, 'abc/') == 'v4'
    assert repo.get3(opt, r0, 'abc/def') == 'v1'
    assert repo.get3(opt, r0, 'abc/def/abc') == 'v3'
    assert repo.get3(opt, r0, 'abcdef/abc') == 'v2'
    assert repo.get3(opt, r0, 'def/') == 'v0'
    assert repo.get3(opt2, r0, 'abcdef/abc') == None
    assert repo.get3(opt2, r2, 'abc/def') == None

  def testLoadRepoConfig(self):
    # -- create test repo config --
    fout = file('repo_cf_test0.py', 'w')
    fout.write("""
#
# Repo config for test0
#

def setup(cf):
  r0 = '/R0'
  r0 = '/R0'
  r1 = '/R1'
  r2 = '/R2'
  opt = 'encoding'
  opt2 = 'encoding2'

  cf.setDefault(opt, 'v0')
  cf.set3(opt, r0, 'abc/', 'v1')
  cf.set3(opt, r0, 'abcdef/', 'v2')
  cf.set3(opt, r0, 'abc/def/', 'v3')
  cf.set3(opt, r1, '', 'v4')
# vim: ts=2 sts=2 expandtab ai
""")
    fout.close()

    repo = LoadRepoConfig('repo_cf_test0')

    r0 = '/R0'
    r1 = '/R1'
    r2 = '/R2'
    opt = 'encoding'
    opt2 = 'encoding2'


    assert repo.get3(opt2, r0, '') == None
    assert repo.get3(opt, r2, 'abc/') == 'v0'
    assert repo.get3(opt, r1, 'abc/') == 'v4'
    assert repo.get3(opt, r0, 'abc/def') == 'v1'
    assert repo.get3(opt, r0, 'abc/def/abc') == 'v3'
    assert repo.get3(opt, r0, 'abcdef/abc') == 'v2'
    assert repo.get3(opt, r0, 'def/') == 'v0'
    assert repo.get3(opt2, r0, 'abcdef/abc') == None
    assert repo.get3(opt2, r2, 'abc/def') == None

    try:
      os.unlink('repo_cf_test0.py')
      os.unlink('repo_cf_test0.pyc')
    except OSError:
      pass
# ---- end of RepoConfigBaseTests 


if __name__ == '__main__':
  unittest.main()
# vim: ts=2 sts=2 expandtab ai
