#import predictionio
#import argparse
#import random
import time
#import pytz
import json
#import dateutil.parser
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
p = r.pubsub()
p.subscribe('storm')

while True:
    message = p.get_message()
    if message:
    	if not isinstance(message['data'], int):
    		data = json.loads(message['data'])
    		print (data)
    		print (data['cookie_smg_uid'] + " -> " + data['target'] + " : " + + data['targetid'])
    	else:
    		print(message)

    time.sleep(0.001)  # be nice to the system :)