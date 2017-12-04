import predictionio
#import argparse
#import random
import time
#import pytz
import json
import dateutil.parser
import redis
#from subprocess import call
import subprocess

count = 0

r = redis.StrictRedis(host='localhost', port=6379, db=0)
p = r.pubsub()
p.subscribe('storm')

client = predictionio.EventClient(
    access_key="zdA_FOUldjIVB3O1r5w44vZdjFTZFyXtQZFxiYeqjZsQ373JW_K8xtwesbA-RBA1",
    url="http://localhost:7070",
    threads=5,
    qsize=500)

while True:
    message = p.get_message()
    if message:
    	if not isinstance(message['data'], int):
    		count += 1
    		data = json.loads(message['data'])
    		#print (data)
    		print (str(count) + ":" + data['cookie_smg_uid'] + " -> " + data['target'] + " => " + data['@timestamp'])
    		data_date = dateutil.parser.parse(data['@timestamp'])

    		client.create_event(
			        event="view",
			        entity_type="user",
			        entity_id=data['cookie_smg_uid'],
			        target_entity_type="item",
			        target_entity_id=data['target'],
			        event_time = data_date
			    )

    		if count >= 10000:
    			#call(["sh","./code/pio-deploy.sh"])
    			subprocess.Popen(["sh", "./code/pio-deploy.sh"])
    			count = 0

    	else:
    		print(message)

    time.sleep(0.001)  # be nice to the system :)