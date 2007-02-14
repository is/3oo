import socket
import time


fout = file('/is/app/o3/log/o3.log', 'a')
sin = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
sin.bind(('0.0.0.0', 50332))

while True:
	try:
		buf = sin.recv(300)

		log = '%s %s' % (time.strftime('%m%d %H:%M:%S'), buf)

		fout.write(log)
		fout.write('\n')
		fout.flush()

		print log
	except KeyboardInterrupt, e:
		break

	except:
		pass

sin.close()
fout.close()
