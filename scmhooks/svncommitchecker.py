# -*- coding: utf8 -*-
#
# -- Subversion Pre Commit Hook --
#

__VERSION__ = '0.0.1.0'
__PROGNAME__ = 'IS Subversion Precommit Checker'

import sys, os
import pysvn

from scmtools import RepoConfig, LoadRepoConfig, FileExtMatch, VersionString

class CommitContext(object):
  def __init__(self):
    self.errors = []
    self.warnings = []
    self.outlines = set()

  def e(self, msg):
    self.errors.append(msg)

  def w(self, msg):
    self.warnings.append(msg)

  def o(self, msg):
    self.outlines.add(msg)

  def isOK(self):
    if len(self.outlines) == 0 and len(self.errors) == 0:
      return True
    else:
      return False
# ---- end of CommitContext



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

  def getChangedFilenames(self):
    txn = self.txn

    txnChanged = txn.changed()
    res = []
    for fn, entry in txnChanged.items():
      if entry[1] != pysvn.node_kind.file:
        continue

      if entry[0] == 'D':
        continue
      res.append(fn)
    return res
  # -- end of getChangedFilenames
      
  def run(self):
    self.setup()

    # -- commit level checks
    self.Check__CommitMessage()

    # -- create changed files list
    fns = self.getChangedFilenames()
    for fn in fns:
      Check__CoreFile(self.ctx, fn)

  def Check__CommitMessage(self):
    ctx = self.ctx
    mesg = ctx.txn.revpropget("svn:log")
    if len(mesg) == 0:
      ctx.o('MSG-O1 请填写完整的提交消息')
      ctx.e('MSG-E1 提交消息为空')
      return

    if len(mesg) < 10:
      ctx.o('MSG-O2 真的没什么可说的吗? 消息长度要大于10')
      ctx.e('MSG-E2 提交消息太短')
      return


# ---- end of CommitChecker


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

# vim: ts=2 sts=2 expandtab ai encoding=utf8
