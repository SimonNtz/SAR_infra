rom __future__ import division
from elasticsearch import Elasticsearch
from pprint import pprint as pp
import requests
import sys
from slipstream.api import Api
import lib_access as la
import server_test as srv3
import numpy as np
import math
api = Api()
#api.login('simon1992', '12mc0v2ee64o9')

index = 'sar7'

type = 'foo3'

server_host = 'localhost'
res = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def get_price(ids, time):
    mapper_unit_price = float(api.cimi_get(ids[0]).json['price:unitCost'])
    reducer_unit_price = float(api.cimi_get(ids[1]).json['price:unitCost'])
    if api.cimi_get(ids[0]).json['price:billingPeriodCode'] == 'HUR' :
      time = math.ceil(time / 3600)
    else:
      time = time/ 3600
    cost = time * (mapper_unit_price + reducer_unit_price)

    return(cost)
def query_db(cloud, time, offer):
    query = { "query":{
          "range" : {
               "%s.execution_time" % offer: {
                      "lte": 980,
                      "gt": 900
                       }
                     }
             }
          }
    return(res.search(index='sar7'))

''' decision making moduke

  : inputs    cloud, offer, time

  : query the document of according clouds for records with execution time equal
    or less than the input time
  : from th

'''

def dmm(cloud, time, offer):
    dtype = '|S64 , |S64, i8'
    ranking = []
    for c in cloud:
        rep = query_db(c, time, offer)['hits']
        pp(rep['hits'][0]['_source']['1'])
    if rep['total']:
      specs = srv3._format_specs(rep['hits'][0]['_source'][offer]['components'])
      time  = rep['hits'][0]['_source'][offer]['execution_time']
      serviceOffers = srv3._components_service_offers(c, specs)
      mapper_so =  serviceOffers['mapper']
      reducer_so =  serviceOffers['reducer']
      cost = get_price([mapper_so, reducer_so], time)
      print c
      ranking.append([c, mapper_so, reducer_so, cost ])
    return sorted(ranking, key=lambda x:x[2])

if __name__ == '__main__':
    cloud=['ec2-eu-west']
    time = 1000
    offer = '1'
    dmm(cloud, time, offer)
