import logging,Queue,threading,time,kafka

log = logging.getLogger("kafka_comsumer")

class KafkaConsumer():
    def __init__(self,topic,group):
        self.kafka= kafka.KafkaClient("10.1.11.50:9092,10.1.11.51:9092,10.1.11.52:9092")
        self.topic=topic
        self.consumer = kafka.MultiProcessConsumer(self.kafka, group,topic,auto_commit=False,auto_commit_every_n=100,auto_commit_every_t=1000,num_procs=2)   
        self.msg_queue = Queue.Queue(maxsize =100000)
    
    def get_msg_queue(self):
        return self.msg_queue
        
    def comsume(self):
        while True:
            try:
                for message in self.consumer:
                    try:
                        log.info("topic=%s,offset=%d"%(self.topic,message.offset))
                        self.msg_queue.put_nowait(message.message)
                    except:
                        time.sleep(10)
                        log.info("topic=%s,offset=%d"%(self.topic,message.offset))
                        self.msg_queue.put_nowait(message.message)
            except Exception,e:
                log.error(e)
                    
    def a_comsume(self):
        thread=threading.Thread(target=self.comsume)
        thread.start()
        return thread