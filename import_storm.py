import predictionio
import argparse
import random
import datetime
import pytz
import json
import dateutil.parser

RATE_ACTIONS_DELIMITER = ","
PROPERTIES_DELIMITER = ":"
SEED = 1


def import_events(client, file):
  f = open(file, 'r')
  #random.seed(SEED)
  count = 0
  ignore = 0
  # year, month, day[, hour[, minute[, second[
  #event_date = datetime.datetime(2015, 8, 13, 12, 24, 41)
  #now_date = datetime.datetime.now(pytz.utc) # - datetime.timedelta(days=2.7)
  #current_date = now_date
  #event_time_increment = datetime.timedelta(days= -0.8)
  print ("Importing data...")

  for line in f:
    linedata = line[1:-2]
    #linedata = str(line)[1,-1]
    #print(linedata)
    print ("running..." + str(count))

    if (',' not in linedata):
      ignore += 1
      continue

    data = linedata.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
    # For demonstration purpose action names are taken from input along with secondary actions on
    # For the UR add some item metadata
    #print("-----" + linedata)

    if len(data) != 3 :
      print("ignore")
      ignore += 1
      continue

    #print(data[0] + "," + data[1] + "," + data[2])
    #print("len:" + str(len(data)))

    if ('T' not in data[2] ):
      print("ignore")
      ignore += 1
      continue

    if (("/" == data[0]) or ("-page" in data[0]) or ("/category" in data[0]) or ("/18-restricted" in data[0]) 
        or ("/articles" in data[0]) or ("/lifestyles" in data[0]) or ("{" in data[0])  
        or ("article" not in data[0] and "lifestyle" not in data[0])):
      print("ignore")
      #print ("target_entity_id: " + data[0] + " entity_id: " + data[1] + " time: " + str(data_date))
      ignore += 1
      continue

    data_date = dateutil.parser.parse(data[2])

    #print ("target_entity_id: " + data[0] + " entity_id: " + data[1] + " time: " + str(data_date))
    count += 1
    
    client.create_event(
        event="view",
        entity_type="user",
        entity_id=data[1],
        target_entity_type="item",
        target_entity_id=data[0],
        event_time = data_date
    )
    

    '''
    client.create_event(
        event="$set",
        entity_type="user",
        entity_id=data[1],
        event_time=data_date,
        properties={}
      )
      '''
    
    '''
    if (data[1] == "purchase"):
      client.create_event(
        event=data[1],
        entity_type="user",
        entity_id=data[0],
        target_entity_type="item",
        target_entity_id=data[2],
        event_time = current_date
      )
      print "Event: " + data[1] + " entity_id: " + data[0] + " target_entity_id: " + data[2] + \
            " current_date: " + current_date.isoformat()
    elif (data[1] == "view"):  # assumes other event type is 'view'
      client.create_event(
              event=data[1],
              entity_type="user",
              entity_id=data[0],
              target_entity_type="item",  # type of item in this action
              target_entity_id=data[2],
              event_time = current_date
      )
      print "Event: " + data[1] + " entity_id: " + data[0] + " target_entity_id: " + data[2] + \
            " current_date: " + current_date.isoformat()
    elif (data[1] == "category-pref"):  # assumes other event type is 'category-pref'
      client.create_event(
              event=data[1],
              entity_type="user",
              entity_id=data[0],
              target_entity_type="item",  # type of item in this action
              target_entity_id=data[2],
              event_time = current_date
      )
      print "Event: " + data[1] + " entity_id: " + data[0] + " target_entity_id: " + data[2] + \
            " current_date: " + current_date.isoformat()
    elif (data[1] == "$set"):  # must be a set event
      properties = data[2].split(PROPERTIES_DELIMITER)
      prop_name = properties.pop(0)
      prop_value = properties if not prop_name == 'defaultRank' else float(properties[0])
      client.create_event(
        event=data[1],
        entity_type="item",
        entity_id=data[0],
        event_time=current_date,
        properties={prop_name: prop_value}
      )
      print "Event: " + data[1] + " entity_id: " + data[0] + " properties/"+prop_name+": " + str(properties) + \
          " current_date: " + current_date.isoformat()
    count += 1
    current_date += event_time_increment
    '''

  f.close()
  print ("%s events are imported." % count)
  print ("%s events are ignore." % ignore)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import storm data for recommendation engine")
  parser.add_argument('--access_key', default='zdA_FOUldjIVB3O1r5w44vZdjFTZFyXtQZFxiYeqjZsQ373JW_K8xtwesbA-RBA1')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/storm.txt")

  args = parser.parse_args()
  print (args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
