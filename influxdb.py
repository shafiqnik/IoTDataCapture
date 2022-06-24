#https://dganais.medium.com/getting-started-with-python-and-influxdb-v2-0-f22e5175aba5

#make sure to run the influxdb  in VM
#goto VM and run influx   then run this script

from datetime import datetime
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Influxdb:

    def __init__(self):

        # make sure the tocket is not expired when you run
        self.token = "fpjzjNNuOkfmlF33RkhA0S4luxUIdwE1bhheE_gzf0rr6aRM3JJz2ediN-nT-nnttz3Cxt0ROp6vSkeRtjNV4g=="
        self.org = "vecima"
        self.bucket = "test"  #make sure bucket is not deleted
        self.client = InfluxDBClient(url="http://10.220.252.10:8086", token=self.token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.sequence = []

    def writedb(self, message, tableName, FixedCollectorID):

        city = 'Vancouver'
        self.tableName = tableName
        self.FixedCollectorID = FixedCollectorID
        self._point1 = Point(tableName).tag("text", city).field(FixedCollectorID, message)
        self.write_api.write(self.bucket, self.org, record=[self._point1])

    def readdb(self):
        query = f'from(bucket:"{self.bucket}")|> range(start:-1h)'
        tables = self.client.query_api().query(query, org=self.org)
        result = self.client.query_api().query(org=self.org, query=query)
        results = []
        for table in result:
            for record in table.records:
                #print(record)
                #print(record.get_measurement())
                #print(record.get_field(),'values ',record.get_value())
                results.append((record.get_field(), record.get_value()))

        print('reading db')
        print('result length', results)
        #prints all the messages from the last hour
        #for message in results:
            #print(message)






mydb = Influxdb()


#Create a table Name: table1 and write the message  (the out put will be [('AC233FC0BFE4', 'this is a test')])
#mydb.writedb("this is a test", "table1","AC233FC0BFE4" )
mydb.readdb()   #reads the messages from the past hour