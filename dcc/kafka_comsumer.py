import logging,Queue,threading,time
from kafka import SimpleConsumer

log = logging.getLogger("kafka_comsumer")

class KafkaConsumer():
    def __init__(self,kafka,topic):
        self.kafka=kafka
        self.topic=topic
        self.consumer = SimpleConsumer(self.kafka, "dcc_python",topic,auto_commit=False,auto_commit_every_n=100)   
        self.msg_queue = Queue.Queue(maxsize =100000)
    
    def get_msg_queue(self):
        return self.msg_queue
        
    def comsume(self):
        while True:
            for message in self.consumer:
                try:
                    log.info(self.topic,message.message) 
                    self.msg_queue.put_nowait(message.message)
                except:
                    time.sleep(10)
                    self.msg_queue.put_nowait(message.message)
                    
    def a_comsume(self):
        thread=threading.Thread(target=self.comsume)
        thread.start()
        return thread