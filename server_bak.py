import time 
import threading 
import Queue


class Worker(threading.Thread):
    def __init__(self, m):
        threading.Thread.__init__(self)
        self.m = m 
    def run(self):
       #PRS check 
        time.sleep(3)
        print self.m
        return(self.m)		

def launch_daemons(q):
    while True:
        msg = q.get() 
        worker = Worker(msg)
        a = worker.start()
	worker.join()
        if isinstance(worker.m, str) and worker.m == 'Rejected':
            print "SLA rejected"
            break
        print "SLA accepted job send to dispatcher: response = %s" % worker.m
    print "Bye"
	      
if __name__ == '__main__':
    queue = Queue.Queue()
    queue.put("Accepted") 
    queue.put("Accepted")
    queue.put("Rejected")
    launch_daemons(queue)

