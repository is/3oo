import os, sys, unittest

from svncommitchecker import CommitContext

class CommitContextTests(unittest.TestCase):
  def testBase0(self):
    ctx = CommitContext()
    ctx.e0('error0')
    ctx.e0('error1')

    ctx.e1('error2')
    ctx.e1('error2')
    ctx.e1('error3')

    ctx.ol('ierror0')
    ctx.ol('ierror1')
    ctx.ol('ierror0')

    assert ctx.error0 == ['error0', 'error1']
    assert ctx.error1 == ['error2', 'error2', 'error3']
    assert ctx.outline == set(('ierror0', 'ierror1'))
# vim: ts=2 sts=2 expandtab ai
