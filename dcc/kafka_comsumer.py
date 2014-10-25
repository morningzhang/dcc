import logging,Queue,threading,time,kafka

log = logging.getLogger("kafka_comsumer")

class KafkaConsumer():
    def __init__(self,topic,group):
        self.kafka= kafka.KafkaClient("10.1.11.50:9092,10.1.11.51:9092,10.1.11.52:9092")
        self.topic=topic
        self.consumer = kafka.SimpleConsumer(self.kafka, group,topic,auto_commit=False)   
        self.msg_queue = Queue.Queue(maxsize =100000)
    
    def get_msg_queue(self):
        return self.msg_queue
        
    def comsume(self):
        while True:
            try:
                for message in self.consumer:
                    try:
                        self.msg_queue.put_nowait(message.message)
                    except:
                        time.sleep(10)
                        self.msg_queue.put_nowait(message.message)
            except Exception,e:
                log.error(e)
                    
    def a_comsume(self):
        thread=threading.Thread(target=self.comsume)
        thread.start()
        return thread
    
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s  %(filename)s  [%(lineno)d]  %(threadName)s  %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]',
                level=logging.INFO)
    
    kc = KafkaConsumer("zhangliming_test", "test")
    kc.a_comsume()
    queue = kc.get_msg_queue()
    while True:
        try:
            print queue.get_nowait()
        except:
            time.sleep(1)
            continue
