#-*-coding:utf-8 -*-  
from thrift import Thrift
from thrift.transport import TSocket  
from thrift.transport import TTransport  
from thrift.protocol import TBinaryProtocol  
from hbase import Hbase
import datetime,time,MySQLdb,struct,sys


class HbaseReader:  
        def __init__(self, address, port, table):  
                self.tableName = table  
                self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))  
                self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)  
                self.client = Hbase.Client(self.protocol)  
                self.transport.open()
  
        def close(self):  
                self.transport.close()  
  
        def read(self, startRow, stopRow, column, attributes, batch_size, mysqlWriter):  
                scannerId = self.client.scannerOpenWithStop(self.tableName, startRow, stopRow, column, attributes)
                while True:
                    try:  
                        result = self.client.scannerGetList(scannerId, batch_size)
                        if len(result)==0:
                            break
                        mysqlWriter.insert_to_mysql(result)
                    except Exception, e:
                        print e             
                self.client.scannerClose(scannerId)  
 
class MysqlWriter:
    def __init__(self,host,user,passwd,db):
        self.mysql_conn=MySQLdb.connect(host,user,passwd,db,charset="utf8")
        self.sql="""insert into dsp_realtime_cpm 
                (partner,hour_time,app_id,ad_id,creative_id,width,height,category,country,os,impression_count,click_count,updated_time) 
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,unix_timestamp()) 
                on duplicate key update impression_count=%s,click_count=%s,updated_time=unix_timestamp()
              """
          
    def insert_to_mysql(self,result):
        items=[]
        for row_result in result:
            columns=[]
            columns.extend(row_result.row.split('u\x001'))
            try:
                impression_count=struct.unpack(">Q",row_result.columns["c:impression"].value)[0]
            except:
                impression_count=0
            try:
                click_count=struct.unpack(">Q",row_result.columns["c:click"].value)[0]
            except:
                click_count=0
            columns.append(impression_count)
            columns.append(click_count)
            columns.append(impression_count)
            columns.append(click_count)
            items.append(columns)
            
        items_count=len(items)
        if items_count>0:
            self.execute(self.mysql_conn,self.sql,items)
            print "insert into mysql =>",items_count
    
    def execute(self,mysql_conn,sql,items):
        try:
            mysql_conn.begin()
            cursor= mysql_conn.cursor() 
            for item in items:
                cursor.execute(sql,item) 
            cursor.close()
            mysql_conn.commit()
        except Exception,_e:
            #self.execute(sql,items)
            print _e
                
def get_timestamp(date,offerset):
        date = datetime.datetime.strptime( date, "%Y%m%d" )
        d = date + datetime.timedelta( days = offerset )
        return int(time.mktime(d.timetuple()))
            

def load_date_data(partner,date):
    startRow="%su\x001%s"%(partner,str(get_timestamp(date, 0)))  
    endRow="%su\x001%s"%(partner,str(get_timestamp(date, 1)))  
    
    hbasereader = HbaseReader("10.1.34.32","9090","dcc")
    hbasereader.read(startRow,endRow,"c",{},100, MysqlWriter("172.20.0.56","ymdsp","123456","ymdsp"))  
    hbasereader.close()
      

if __name__ == "__main__":  
    load_date_data(sys.argv[1],sys.argv[2])
