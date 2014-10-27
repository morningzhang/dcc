import logging,kafka,uuid

log = logging.getLogger("kafka_producter")

class KafkaProducer():
    def __init__(self,topic,group):
        self.kafka= kafka.KafkaClient("10.1.11.50:9092,10.1.11.51:9092,10.1.11.52:9092")
        self.topic=topic
        self.producer = kafka.SimpleProducer(self.kafka, async=False,
                          req_acks=kafka.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                          ack_timeout=2000)

    def send(self,kafkalog,count):
        while True:
            try:
                klog=kafkalog%uuid.uuid4()
                print klog
                self.producer.send_messages(self.topic, klog)
                count=count-1
                if count<=0:
                        break   
            except Exception,e:
                log.error(e)
                
    def close(self):
        self.kafka.close()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s  %(filename)s  [%(lineno)d]  %(threadName)s  %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]',
                level=logging.INFO)
    
    kp = KafkaProducer("dspclick", "test")
    kafkalog = "[2014-10-27 12:43:53,437] {\"adId\":48,\"advertiserAppId\":\"\",\"appId\":\"11111\",\"auctionId\":\"%s\",\"category\":\"\",\"cb\":\"0.9136349444743246\",\"country\":\"CHN\",\"creativeId\":33,\"deviceId\":\"f3b32d2cf9bd7c89bc61d9f7bc5f2b15f5e087dc\",\"hash\":\"\",\"height\":50,\"ip\":\"223.104.9.109\",\"lat\":0,\"lng\":0,\"os\":\"ios\",\"partner\":\"smaato\",\"timeCost\":2,\"title\":\"click\",\"width\":320}\n";
    kp.send(kafkalog, 1000)
    kp.close()