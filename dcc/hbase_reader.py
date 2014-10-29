#-*-coding:utf-8 -*-  
from thrift import Thrift
from thrift.transport import TSocket  
from thrift.transport import TTransport  
from thrift.protocol import TBinaryProtocol  
from hbase import Hbase
import datetime,time
  
class HbaseWriter:  
        def __init__(self, address, port, table):  
                self.tableName = table  
                self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))  
                self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)  
                self.client = Hbase.Client(self.protocol)  
                self.transport.open()
  
        def close(self):  
                self.transport.close()  
  
        def read(self,startRow,stopRow,column,attributes,batch_size,cb):  
                scannerId = self.client.scannerOpenWithStop(self.tableName, startRow, stopRow, column,attributes)
                while True:  
                        try:  
                                result = self.client.scannerGetList(scannerId, batch_size)
                                cb(result)
                        except Exception,e:
                                print e  
                                continue          
                        
                self.client.scannerClose(scannerId)  
                
        def get_timestamp(self,date,offerset):
                date = datetime.datetime.strptime( date, "%Y%m%d" )
                d = date + datetime.timedelta( days = offerset )
                return int(time.mktime(d.timetuple()))
  
  
  
if __name__ == "__main__":  
        date="20141025"
        client = HbaseWriter("10.1.34.32","9090","dcc")
        
        startTime=client.get_timestamp(date, 0)
        endTime=client.get_timestamp(date, 1)
        def print_result(result):
            print len(result)

        client.read("smaatou\x001%s"%str(startTime),"smaatou\x001%s"%str(endTime),"c",{},100,print_result)  
        client.close()
