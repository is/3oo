# -*- coding: utf8 -*-
#
# -- Subversion Pre Commit Hook --
#

__VERSION__ = '0.0.1.1'
__PROGNAME__ = 'IS Subversion Precommit Checker'

import sys, os
import pysvn

from scmtools import RepoConfig, LoadRepoConfig, FileExtMatch, VersionString, FileExt

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
# --CEND--


class CommitChecker(object):
  def __init__(self, cf, repoPath, txnid):
    self.cf = cf
    self.repoPath = repoPath
    self.txnid = txnid

  def setup(self):
    self.txn = pysvn.Transaction(self.repoPath, self.txnid)

    # Create context
    self.ctx = CommitContext()
    self.ctx.repoPath = self.repoPath
    self.ctx.txnid =  self.txnid
    self.ctx.txn = self.txn
    self.ctx.cf = self.cf
  # --end--

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
  # --end--

  def checkFileConfig(self, opt, repoPath, path):
    cf = self.cf
    ext = FileExt(path)

    if ext == '':
      return True

    exts = cf.get3(opt, repoPath, path)
    return FileExtMatch(exts, ext)
  # --end--

  def isBinaryFileByConfig(self, repoPath, path):
    return self.checkFileConfig('binary-ext', repoPath, path)
  # --end--

  def isSourceFileByConfig(self, repoPath, path):
    return self.checkFileConfig('source-ext', repoPath, path)
  # --end--

  def isBinaryFile(self, path):
    if self.isBinaryFileByConfig(self.ctx.repoPath, path):
      return True
    # TODO: check svn props
    return False
  # --end--

  def run(self):
    self.setup()
    ctx = self.ctx

    # -- commit level checks
    self.Check__CommitMessage()

    # -- create changed files list
    fns = self.getChangedFilenames()
    for fn in fns:
      self.Check__FileCore(fn)

    if ctx.isOK():
      return
      
    print >> sys.stderr, '--ERRORS--'
    print >> sys.stderr, '\n'.join(ctx.errors)
    print >> sys.stderr, '\n--OUTLINES--'
    print >> sys.stderr, '\n'.join(ctx.outlines)

    sys.exit(1)
  # --end--


  def Check__FileCore(self, path):
    cf = self.cf
    ctx = self.ctx
    txn = ctx.txn
    repoPath = ctx.repoPath

    if self.isBinaryFile(path):
      # binary file is passed directly.
      return

    if cf.get3('check-utf8', repoPath, path):
      if self.isSourceFileByConfig(repoPath, path):
        self.Check__UTF8(path)

        if cf.get3('check-bom', repoPath, path):
          self.Check__BOM(path)
  # --end--

  def Check__BOM(self, path):
    ctx = self.ctx
    content = self.ctx.txn.cat(path)
    upath = path.encode('utf8')
    if content[:3] == '\xef\xbb\xbf':
      ctx.e("BOM-E1 %s 含有Unicode BOM头标志" % upath)
      ctx.o("BOM-O1 请清除相关文件的BOM头")
    return

  # --end--

  def Check__UTF8(self, path):
    ctx = self.ctx
    content = self.ctx.txn.cat(path)

    linenum = 1
    lines = []
    texts = content.split("\n")

    for line in texts:
      try:
        line.decode('utf8')
      except UnicodeDecodeError, e:
        lines.append(str(linenum))
      linenum += 1

    upath = path.encode("utf8")
    if lines:
      ctx.e("UTF-E1 %s 包含非法的UTF8字符(文件必须是UTF8编码)" % (upath))
      ctx.e("UTF-E1 %s 存在问题的行: %s" % (upath, ",".join(lines)))
      ctx.o("UTF-O1 请仔细检查并修正文件编码问题")
  # --end--

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
# --CEND--

def Main():
  print >>sys.stderr, '= %s (v%s)' % (__PROGNAME__, __VERSION__)
  print >>sys.stderr, '  python-%s, pysvn-%s, subversion-%s' % (
    VersionString(sys.version_info[:3]),
    VersionString(pysvn.version), 
    VersionString(pysvn.svn_version[:3]))
  if len(sys.argv) < 4:
    print >>sys.stderr, '! is-precommit-checker need three command line arguments:'
    print >>sys.stderr, '!   python is-precommit {config_name} {repo_path} {txnid}'
    sys.exit(1)
  
  cf =  LoadRepoConfig(sys.argv[1])
  checker = CommitChecker(cf, sys.argv[2], sys.argv[3])
  checker.run()
  sys.exit(0)
# ---- end of Main
  
if __name__ == '__main__':
  Main()

# vim: ts=2 sts=2 expandtab ai encoding=utf8
