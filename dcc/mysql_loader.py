import MySQLdb,Queue,logging,time,threading

log = logging.getLogger("load_data")

class MysqlLoader():
    def __init__(self,column):
        self.mysql_conn=MySQLdb.connect(host="127.0.0.1",user="root",passwd="111111",db="dsp_report",charset="utf8") 
        self.column=column  
               
    def load_from_queue(self,queue):
        sql_prefix="""
        replace into dsp_realtime_cpm 
        (`partner`,`hour_time`,`app_id`,`ad_id`,`creative_id`,`width`,`height`,`category`,`country`,`os`,`%s`,`updated_time`) 
        values 
        """%self.column
        
        messages=[];mysql_idle_time=0
        while True:
            try:
                message=queue.get_nowait()
                messages.append(message)
                msglen=len(messages)
                if msglen>=100:
                    log.info('items_len = %d update database.',msglen)
                    self.commit_to_db(sql_prefix,messages)
                    messages=[]
                    mysql_idle_time=0
            except:
                msglen=len(messages)
                if msglen==0:
                    log.info('no data,sleeping 10 seconds.')
                    #idle time
                    mysql_idle_time+=10
                    if mysql_idle_time>=3600:
                        self.execute("select 0")
                        mysql_idle_time=0
                    time.sleep(10)
                    continue
                
                log.info('no data,items_len = %d update database.',msglen)
                self.commit_to_db(sql_prefix,messages)
                messages=[]
                mysql_idle_time=0
                
    def commit_to_db(self,sql_prefix,messages):
            values=[]
            for message in messages:
                    columns=message.key.split('u\x001')
                    columns.append(message.value)
                    values.append("('%s',%s,'%s',%s,%s,%s,'%s','%s','%s','%s',%s,unix_timestamp())"%tuple(columns))
            self.execute(sql_prefix+",".join(values))
            
                                
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    class Message():
        def __init__(self,key,value):
            self.key=key
            self.value=value
    
    queue=Queue.Queue(maxsize = 100000)
    for i in xrange(100):
        queue.put_nowait(Message("smaatou\x0011414119600u\x001www.local.com%du\x001269u\x0018083u\x001320u\x00150u\x001IAB1u\x001USAu\x001android"%i,"9426"))

    loader=MysqlLoader('impression_count')
    loader.load_from_queue(queue)