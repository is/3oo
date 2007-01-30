from o3lib import fs
import Queue

queue = Queue.Queue()

fs.O3EntityReader(queue,
	label = '0',
	node = 'p-dx69',
	name = 'test/TEST.iz0',
	addr = '10.6.39.218',
	entityid = 4498)

fout = file('/tmp/TEST_ER01', 'wb')
while True:
	c = queue.get()
	if not c:
		break
	print len(c)
	fout.write(c)

fout.close()

	
