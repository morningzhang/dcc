import logging,sys 
import kafka_comsumer
import mysql_loader

kafka_group="dcc_python_group"
if len(sys.argv)>2:
    kafka_group=sys.argv[1]

#log
logging.basicConfig(format='%(asctime)s - %(levelname)s  %(filename)s  [%(lineno)d]  %(threadName)s  %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]',
                    filename='dcc.log', level=logging.INFO)

threads=[]

topics=[("dcc_impression","impression_count"),("dcc_click","click_count")]
for topic in topics:
    comsumer=kafka_comsumer.KafkaConsumer(topic[0],kafka_group)
    threads.append(comsumer.a_comsume())
    loader=mysql_loader.MysqlLoader(topic[1])
    threads.append(loader.a_load_from_queue(comsumer.get_msg_queue()))
    

for thread in threads:
    thread.join()
    
logging.info("run now....")