#!/usr/bin/python

from o3grid.utility import cout

class IsGrid0Job(object):
	def run(self):
		cout("I'm iSGrid0.Job")

cout("Load IsGrid0.IsGrid0")

def generateJob(jobinfo, workspace):
	return IsGrid0Job()
