import Queue,logging,time,threading,leveldb

log = logging.getLogger("load_data")

class LeveldbLoader():
    def __init__(self,topic): 
        self.topic=topic
        self.topicdb = leveldb.LevelDB(topic)
               
    def load_from_queue(self,queue):
        while True:
            try:
                message=queue.get_nowait()
                self.put_to_topicdb(message)
            except:
                time.sleep(10)
                log.info('no data,sleeping 10 seconds.')
                
    def put_to_topicdb(self,message):
        key="%su\x001%s"%(str(self.get_3600_timestamp()),message.key);
        try:
            value = self.topicdb.Get(key)
            if value < message.value:
                self.topicdb.Put(key, message.value)
        except:
            self.topicdb.Put(key, message.value)
            
    def get_3600_timestamp(self):
        timestamp=int(time.time())
        return timestamp-timestamp%3600
    
    
    def a_load_from_queue(self,queue):
        thread=threading.Thread(target=self.load_from_queue,args=(queue,))
        thread.start()
        return thread 

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    class Message():
        def __init__(self,key,value):
            self.key=key
            self.value=value
    
    queue=Queue.Queue(maxsize = 100000)
    for i in xrange(100):
        queue.put_nowait(Message("smaatou\x0011414119600u\x001www.local.com%du\x001269u\x0018083u\x001320u\x00150u\x001IAB1u\x001USAu\x001android"%i,"8888"))

    loader=LeveldbLoader('impression_count')
    loader.a_load_from_queue(queue)
    