'''
This mqtt subscriber application listens to topic
Writes the messages in influxdb

Supporting libraries
dnspython 2.0.0
paho-mqtt 1.5.1
pip 20.2.3
setuptools 49.2.1
'''

#!/usr/bin/python3

from datetime import date

import datetime
import time
import sys
from time import gmtime, strftime
from influxdb import Influxdb
import json

#include the library path to system path. Not required if libraries are installed
sys.path.insert(0, "c:/python39/lib/site-packages")
import paho.mqtt.client as mqtt  #import paho mqtt


mytime = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
timestamp = str(datetime.datetime.now())
mydb = Influxdb()

#Variable for MQTT connection and storage

mqttBroker ="104.36.12.156"
portNumber = 18160

#mqttBroker="10.220.252.10"
topic = 'net1'
client = mqtt.Client("laptop")

def convertList_to_dic(lst):
    it = iter(a)
    res_dct = dict(zip(it, it))
    return res_dct



def on_message(client, userdata, message):
    ble_payload = str(message.payload.decode("utf-8"))
    y = json.loads(message.payload.decode("utf-8"))
    #print('y  0 is  ', y[0]) #this inlcudes gateway id
    #print('gateway id is ', y[0]['mac'])  #prints the mac address of the gateway
    if len(y)> 1:
        print('fcid',y[0]['mac'],'tag id ', y[1]['mac'],y[1]['rssi'], y[1]['timestamp'])  #scanned tag1 info
    #print('y is ', len(y)) #number of tags scanned



    mymessage = ble_payload.split(',')
    if len(mymessage) > 7:
       # dbinsert(mymessage[2],mymessage[1],mymessage[3:10], mymessage[-1])
        #print(mymessage)


        tagInfo = y[1]['mac']+'-'+y[1]['timestamp']
        mydb.writedb(y[0]['mac'], y[1]['rssi'],tagInfo)


    else:
        #print(mymessage)
        print('xxx')


client.connect(mqttBroker,port=portNumber)
print('Connected to MQTT broker...')


# Continue to read the messages
while True:
    client.loop_start()
    client.subscribe(topic)
    client.on_message=on_message
    time.sleep(30)
    client.loop_stop()

