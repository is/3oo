#
# -- Subversion Pre Commit Hook
#

__VERSION__ = '0.0.1.0'
__PROGNAME__ = 'IS Subversion Precommit Checker'

import sys, os
import pysvn

from scmtools import RepoConfig, LoadRepoConfig, FileExtMatch, VersionString

class CommitContext(object):
  def __init__(self):
    self.error0 = []
    self.error1 = []
    self.outline = set()

  def e0(self, msg):
    self.error0.append(msg)

  def e1(self, msg):
    self.error1.append(msg)

  def ol(self, msg):
    self.outline.add(msg)

class CommitChecker(object):
  def __init__(self, cf, repoPath, txnid):
    self.cf = cf
    self.repoPath = repoPath
    self.txnid = txnid

  def setup(self):
    self.txn = pysvn.Transation(self.repoPath, self.txnid)

    # Create context
    self.ctx = CommitContext()
    self.ctx.repoPath = self.repoPath
    self.ctx.txnid =  self.txnid
    self.ctx.txn = self.txn
    self.ctx.cf = self.cf

  def run(self):
    self.setup()

def Main():
  print '= %s (v%s)' % (__PROGNAME__, __VERSION__)
  print '  python-%s, pysvn-%s, subversion-%s' % (
    VersionString(sys.version_info[:3]),
    VersionString(pysvn.version), 
    VersionString(pysvn.svn_version[:3]))
  if len(sys.argv) < 4:
    print '! is-precommit-checker need three command line arguments:'
    print '!   python is-precommit {config_name} {repo_path} {txnid}'
    sys.exit(1)
  
  cf =  LoadRepoConfig(sys.argv[1])
  checker = CommitCheckerContext(cf, sys.argv[2], sys.argv[3])
  checker.run()
  sys.exit(0)
# ---- end of Main
  
if __name__ == '__main__':
  Main()

# vim: ts=2 sts=2 expandtab ai
