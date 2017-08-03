from elasticsearch import Elasticsearch
import requests
import json
from datetime import datetime

res = Elasticsearch([{'host': 'localhost', 'port': 9200}])
r = requests.get('http://localhost:9200')

if str(r) != '<Response [200]>':
    print "Can't speak to elasticsearch"
def groupby_duiid(duiid):
  index='_all'
  run = res.search(index='_all',
                 body={"query":{"match": {'fields.duiid':duiid}}})
  return(run)

def extract_time(m):
  temp = m[0].split(' - ')[1].strip()
  return(datetime.strptime(temp, "%H:%M:%S"))

def compute_execution_time(mapper_msg, reducer_msg):
  msg_start  = filter(lambda x:'start deployment' in x, mapper_msgs)
  msg_end    = filter(lambda x:'start upload' in x, reducer_msgs)
  delta     =  extract_time(msg_end) - extract_time(msg_start)

  return(str(delta.seconds))

def div_per_field(hits, field, value):
  if hits['total'] > 0 :
    hitsObj = hits['hits']
    result = [h for h in hitsObj \
            if h['_source']['fields'][field] == value]
  else:
    result={}
  return result

def create_run_input(duiid):
    duiid = x

    run_group = groupby_duiid(duiid)
    mapper  = div_per_field(hits1['hits'], 'nodename', 'mapper')
    reducer = div_per_field(hits1['hits'], 'nodename', 'reducer')

    mapper_msgs=[h['_source']['message'] for h in mapper['hits']['hits']]
    reducer_msgs=[h['_source']['message'] for h in reducer['hits']['hits']]

    cloud = mapper[0]['_source']['fields']['cloud']
    serviceOffer = mapper[0]['_source']['fields']['service-offer']

    time = compute_execution_time(mapper_msgs, reducer_msgs)

    run = { 'run_id': duiid,
            'cloud':, cloud,
            'time': {'provisioning':,'install', 'deployment' }
            'service-offer'serviceOffer,
            'message':{'mapper': mapper_msgs, 'reducer':reducer_msgs},
            'products':{'name', }
             }



def div_per_field(hits, field, value):
  if hits['total'] > 0 :
    hitsObj = hits['hits']
    result = [h for h in hitsObj \
              if h['_source']['fields'][field] == value]
  else:
    result={}
  return result



def query_time_per_cloud(cloud, time):

    cloud_query = []
    for c in cloud:
      cloud_query.append = {"term":{"cloud": }}


    query = {
              "query": {
                  "bool": {
                          "must": [
                              {"match":{"fields.cloud" : "eo-cesnet-cz1"}},
                              {"match_phrase":{"message": "finish processing"}}
                          ]
                        }
                    }
              }
    response = res.search(index='_all', body = query)
    result = []
    for m in response['hits']['hits']:
        if m['_source']['message'].split(' - ')[-1] <= time:
            results.append(m['fields']['duiid'])







res = Elasticsearch([{'host': 'localhost', 'port': 9200}])

query = {
        "query": {
                "bool": {
                        "must": [
                            {"match":{"fields.cloud" : "eo-cesnet-cz1"}},
                            {"match_phrase":{"message": "finish processing"}}
                        ]
}
}
}


query1 = {
         "query": {
             "match_phrase": { "message":"start deployment" }
             }
}

rep = res.search(index='_all', body=query)
pp(rep)
