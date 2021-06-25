#https://dganais.medium.com/getting-started-with-python-and-influxdb-v2-0-f22e5175aba5

from datetime import datetime
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Influxdb:

    def __init__(self):
        self.token = "4RbyPCLhQ2kAqQKnAC57eDo9C47UCJzQuGqpka6hJ5Xqv7xqouTmiKlwcMGHart_2PCjUcACqd3W_OEVZz4Fcg=="
        self.org = "vecima"
        self.bucket = "Time"
        self.client = InfluxDBClient(url="http://10.220.252.10:8086", token=self.token)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.sequence = []

    def writedb(self, message, tableName, FixedCollectorID):

        city = 'Tokyo'
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
                results.append((record.get_field(), record.get_value()))

        print('reading db')
        print('result length', results[-1])
        #prints all the messages from the last hour
        #for message in results:
            #print(message)






mydb = Influxdb()
mydb.readdb()