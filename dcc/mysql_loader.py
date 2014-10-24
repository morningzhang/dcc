import logging,time,threading

log = logging.getLogger("load_data")

class MysqlLoader():
    def __init__(self,mysql_conn,column):
        self.mysql_conn=mysql_conn
        self.column=column  
        
        
    def load_from_queue(self,queue):
        sql_prefix="""
        replace into dsp_realtime_cpm 
        (`partner`,`hour_time`,`app_id`,`ad_id`,`creative_id`,`width`,`height`,`category`,`country`,`os`,`%s`) 
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
            except:
                if len(items)==0:
                    time.sleep(100)
                    continue
                values=[]
                for k,v in items.items():
                    values.append("('%s','%s',%s,%s,%s,%s,'%s','%s','%s','%s',%s)"%tuple(k.split(u'\x001').append(v)))
                self.execute(sql_prefix+values.join(","))
                items.clear()
                time.sleep(100)
            
    def a_load_from_queue(self,queue):
        thread=threading.Thread(target=self.load_from_queue,args=(queue,))
        thread.start()
        return thread 
            
    def load_from_file(self,data_file):
        sql="""
             LOAD DATA LOCAL INFILE '%(data_file)s' INTO TABLE dsp_realtime_cpm CHARACTER SET utf8
             FIELDS TERMINATED BY ',' 
             OPTIONALLY ENCLOSED BY '\"' 
             LINES TERMINATED BY '\n'
             (partner 
            ,app_id 
            ,ad_id 
            ,creative_id
            ,width 
            ,height 
            ,category
            ,country 
            ,os 
            ,hour_time
            ,%(column)s) set updated_time=unix_timestamp();
            """%{"data_file":data_file,"column":self.column}
        
        self.execute(sql)
           
    def a_load_from_file(self,data_file):
        thread=threading.Thread(target=self.load_from_file,args=(data_file,))
        thread.start()
        return thread         
                            
    def execute(self,sql):
        try:
            self.mysql_conn.begin()
            cursor= self.mysql_conn.cursor() 
            n=cursor.execute(sql) 
            log.info("effect %d rows."%n)
        except:
            self.mysql_conn.rollback()
        finally:
            if cursor!=None:
                cursor.close()
            self.mysql_conn.commit()