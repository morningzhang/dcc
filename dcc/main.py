import kafka,MySQLdb,logging,sys 
import kafka_comsumer
import mysql_loader

#log
logging.basicConfig(format='%(asctime)s - %(levelname)s  %(filename)s  [%(lineno)d]  %(threadName)s  %(message)s', datefmt='[%Y-%m-%d %H:%M:%S]',
                    filename='dcc.log', level=logging.INFO)
#kafka
kafka_client = kafka.KafkaClient("10.1.11.50:9092,10.1.11.51:9092,10.1.11.52:9092")

#mysql
mysql_conn=MySQLdb.connect(host="172.20.0.56",user="ymdsp",passwd="123456",db="ymdsp",charset="utf8") 


threads=[]

topics=[("dcc_impression","impression_count"),("dcc_click","click_count")]
for topic in topics:
    comsumer=kafka_comsumer.KafkaConsumer(kafka_client,topic[0],sys.argv[1] or "dcc_python_group")
    threads.append(comsumer.a_comsume())
    loader=mysql_loader.MysqlLoader(mysql_conn,topic[1])
    threads.append(loader.a_load_from_queue(comsumer.get_msg_queue()))
    

for thread in threads:
    thread.join()
    
logging.info("run now....")