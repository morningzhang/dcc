import MySQLdb,Queue,logging,time,threading

log = logging.getLogger("load_data")

class MysqlLoader():
    def __init__(self,mysql_conn,column):
        self.mysql_conn=mysql_conn
        self.column=column  
               
    def load_from_queue(self,queue):
        sql_prefix="""
        replace into dsp_realtime_cpm 
        (`partner`,`hour_time`,`app_id`,`ad_id`,`creative_id`,`width`,`height`,`category`,`country`,`os`,`%s`,`updated_time`) 
        values 
        """%self.column
        
        items={}
        while True:
            try:
                message=queue.get_nowait()
                try:
                    value=items[message.key]
                    if value<message.value:
                        items[message.key]=message.value
                except:
                    items[message.key]=message.value
                items_len=len(items)
                if items_len>=100:
                    log.info('items_len = %d update database.',items_len)
                    self.commit_to_db(sql_prefix,items)
            except:
                if len(items)==0:
                    log.info('no data,sleeping 10 seconds.')
                    time.sleep(10)
                    continue
                log.info('no data,items_len = %d update database.',len(items))
                self.commit_to_db(sql_prefix,items)
                
    def commit_to_db(self,sql_prefix,items):
            values=[]
            for k,v in items.items():
                    columns=k.split('u\x001')
                    columns.append(v)
                    values.append("('%s',%s,'%s',%s,%s,%s,'%s','%s','%s','%s',%s,unix_timestamp())"%tuple(columns))
            self.execute(sql_prefix+",".join(values))
            items.clear()
                                
    def execute(self,sql):
        try:
            self.mysql_conn.begin()
            cursor= self.mysql_conn.cursor() 
            n=cursor.execute(sql) 
            log.info("effect %d rows."%n)
            cursor.close()
        except Exception,e:
            print e
            self.mysql_conn.rollback()
        finally:
            self.mysql_conn.commit()
            
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
    
    mysql_conn=MySQLdb.connect(host="172.20.0.56",user="ymdsp",passwd="123456",db="ymdsp",charset="utf8") 
    loader=MysqlLoader(mysql_conn,'impression_count')
    loader.load_from_queue(queue)
