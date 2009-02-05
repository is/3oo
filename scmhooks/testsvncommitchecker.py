import os, sys, unittest
import pysvn
from mock import Mock

from svncommitchecker import CommitContext
from svncommitchecker import CommitChecker
from scmtools import RepoConfig

class DummyClass(object): pass

class CommitContextTests(unittest.TestCase):
  def testBase0(self):
    ctx = CommitContext()
    ctx.d('debug0')
    ctx.d('debug1')
    
    ctx.e('error0')
    ctx.e('error1')

    ctx.w('error2')
    ctx.w('error2')
    ctx.w('error3')

    ctx.o('ierror0')
    ctx.o('ierror1')
    ctx.o('ierror0')

    assert ctx.debugs == ['debug0', 'debug1']
    assert ctx.errors == ['error0', 'error1']
    assert ctx.warnings == ['error2', 'error2', 'error3']
    assert ctx.outlines == set(('ierror0', 'ierror1'))
  # -- end

  def testIsOK(self):
    c0 = CommitContext()
    assert c0.isOK() == True

    c0.w('warning0')
    assert c0.isOK() == True

    c0.e('error0')
    assert c0.isOK() == False

    c0.errors = []
    assert c0.isOK() == True

    c0.o('outline0')
    assert c0.isOK() == False
# ----end----

class MessageCheckerTests(unittest.TestCase):
  def createContext(self, txn):
    ctx = CommitContext()
    ctx.txn = txn
    return ctx
  # --end--

  def createContextByMessage(self, msg):
    txn = DummyClass()
    txn.revpropget = Mock()
    txn.revpropget.return_value = msg
    ctx = self.createContext(txn)
    return ctx
  # --end--

  def mockChecker(self, msg):
    checker = CommitChecker(None, None, None)
    checker.ctx = self.createContextByMessage(msg)
    return checker

  def testOkMessage(self):
    cc = self.mockChecker(u'hello-world, this is a good message')
    cc.Check__CommitMessage()
    assert cc.ctx.isOK()

  def testEmptyMessage(self):
    cc = self.mockChecker(u'')
    cc.Check__CommitMessage()

    ctx = cc.ctx
    assert not ctx.isOK() 
    assert ctx.errors[0].split()[0] == 'MSG-E1'

  def testShortenMessage(self):
    cc = self.mockChecker(u'shortmsg')
    cc.Check__CommitMessage()
    ctx = cc.ctx
    assert not ctx.isOK()
    assert ctx.errors[0].split()[0] == 'MSG-E2'
# ----end----


class CommitCheckerTests(unittest.TestCase):
  def mockContext0(self, changed):
    ctx = CommitContext()
    ctx.txn = DummyClass()
    ctx.txn.changed = Mock()
    ctx.txn.changed.return_value = changed
    return ctx
    # --end--
  
  def testChangeFilenames(self):
    ctx = self.mockContext0({
      'is/a': ('D', pysvn.node_kind.file, 1, 0),
      'is/b': ('R', pysvn.node_kind.file, 1, 0),
      'is/c': ('A', pysvn.node_kind.dir, 1, 0),
      'is/d': ('A', pysvn.node_kind.file, 1, 1),
    })

    cc = CommitChecker(None, None, None)
    cc.ctx = ctx
    cc.txn = ctx.txn
    assert set(cc.getChangedFilenames()) == set(['is/b', 'is/d'])
  # --end--

  def mockChecker2(self, repoPath, cf):
    cc = CommitChecker(cf, repoPath, None)
    ctx = DummyClass()
    cc.ctx = ctx
    ctx.repoPath = repoPath
    cc.cf = cf
    return cc
  # --end--

  def testIsBinaryFileByConfig(self):
    R0 = '/R0'
    cf = RepoConfig()
    cf.setDefault('binary-ext', 'obj,lib,html,js')
    cf.set3('binary-ext', R0, 'abc/', 'rmvb,avi,txt')
    cf.set3('binary-ext', R0, 'abc/def/', 'sln,lib')
    cf.set3('binary-ext', R0, 'abcdef/', '+')

    cc = self.mockChecker2(R0, cf)
    assert cc.isBinaryFileByConfig(R0, 'abc/def.avi') == True
    assert cc.isBinaryFileByConfig(R0, 'abc/def.java') == False
    assert cc.isBinaryFileByConfig(R0, 'abcdef/test.abc') == True
    assert cc.isBinaryFileByConfig(R0, 'abc/defhgi') == True
    assert cc.isBinaryFileByConfig(R0, 'abc/def/ssh.cpp') == False
    assert cc.isBinaryFileByConfig(R0, 'abc/def/ssh.lib') == True
# --cend--

# vim: ts=2 sts=2 expandtab ai
