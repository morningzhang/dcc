import MySQLdb,Queue,logging,time,threading,leveldb

log = logging.getLogger("load_data")

class MysqlLoader():
    def __init__(self,column):
        #self.mysql_conn=MySQLdb.connect(host="127.0.0.1",user="root",passwd="111111",db="dsp_report",charset="utf8")
        self.mysql_conn=MySQLdb.connect(host="172.20.0.56",user="ymdsp",passwd="123456",db="ymdsp",charset="utf8")  
        self.column=column
        self.columndb = leveldb.LevelDB(column)
               
    def load_from_queue(self,queue):
        while True:
            try:
                message=queue.get_nowait()
                self.put_to_columndb(message)
            except:
                time.sleep(10)
                log.info('no data,sleeping 10 seconds.')
                
    def put_to_columndb(self,message):
        key="%su\x001%s"%(str(self.get_60_timestamp()),message.key);
        try:
            value = self.columndb.Get(key)
            if value < message.value:
                self.columndb.Put(key, message.value)
        except:
            self.columndb.Put(key, message.value)
            
    def get_60_timestamp(self):
        timestamp=int(time.time())
        return timestamp-timestamp%60
    
    def commit_db(self):
        while True:
            timestamp=self.get_60_timestamp()
            for item in self.columndb.RangeIter(key_from = str(timestamp-60), key_to = str(timestamp)):
                columns=[]
                columns.append(self.column)
                columns.extend(item[0].split('u\x001')[1:])
                columns.append(item[1])
                columns.append(self.column)
                columns.append(item[1])
                sql="""
                insert into dsp_realtime_cpm 
                (partner,hour_time,app_id,ad_id,creative_id,width,height,category,country,os,%s,updated_time) 
                values ('%s',%s,'%s',%s,%s,%s,%s,'%s','%s','%s',%s,unix_timestamp()) on duplicate key update %s=%s,updated_time=unix_timestamp()
                """
                self.execute(sql%tuple(columns))
            time.sleep(60)
            
                                
    def execute(self,sql):
        try:
            self.mysql_conn.begin()
            cursor= self.mysql_conn.cursor() 
            n=cursor.execute(sql) 
            log.info("effect %d rows."%n)
            cursor.close()
            self.mysql_conn.commit()
        except Exception,e:
            log.error(e)
            log.error("retry...")
            self.execute(sql)
            
    def a_load_from_queue(self,queue):
        thread=threading.Thread(target=self.load_from_queue,args=(queue,))
        thread.start()
        return thread 
    
    def a_commit_db(self):
        thread=threading.Thread(target=self.commit_db)
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

    loader=MysqlLoader('impression_count')
    loader.a_load_from_queue(queue)
    loader.a_commit_db()
    