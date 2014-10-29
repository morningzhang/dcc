import MySQLdb,logging,time,threading

log = logging.getLogger("mysql_loader")

class MysqlLoader():
    def __init__(self,topicdb,column):
        #self.mysql_conn=MySQLdb.connect(host="127.0.0.1",user="root",passwd="111111",db="dsp_report",charset="utf8") 
        self.column=column
        self.topicdb = topicdb
        self.mysql_conn=MySQLdb.connect(host="172.20.0.56",user="ymdsp",passwd="123456",db="ymdsp",charset="utf8") 
            
    
    def commit_db(self):
        sql="""insert into dsp_realtime_cpm 
                (partner,hour_time,app_id,ad_id,creative_id,width,height,category,country,os,%(column)s,updated_time) 
                values (%(values)s) 
                on duplicate key update %(column)s=%(columnvalue)s,updated_time=unix_timestamp()
                """%{"column":self.column,"values":"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,unix_timestamp()","columnvalue":"%s"}
        while True:
            items=[]
            for item in self.topicdb.RangeIter(key_from = str(int(time.time())-3600), key_to = str(int(time.time()))):
                columns=[]
                columns.extend(item[0].split('u\x001')[1:])
                columns.append(item[1])
                columns.append(item[1])
                items.append(columns)
            self.execute(sql,items)
            time.sleep(60)
            
                                
    def execute(self,sql,items):
        try:
            self.mysql_conn.begin()
            cursor= self.mysql_conn.cursor() 
            for item in items:
                cursor.execute(sql,item) 
            cursor.close()
            self.mysql_conn.commit()
        except Exception,e:
            log.error(e)
            log.error("retry...")
            self.execute(sql,items)
            
    def a_commit_db(self):
        thread=threading.Thread(target=self.commit_db)
        thread.start()
        return thread 

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    leveldbloader=MysqlLoader('dcc_impression','impression_count')
    leveldbloader.a_commit_db()
    