from elasticsearch import Elasticsearch
import requests
import sys
from datetime import datetime
from pprint import pprint as pp
from collections import defaultdict

server_host = 'localhost'

res = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def _extract_time(m):
    temp = m[0].split(' - ')[1].strip()
    if len(temp.split('T')) > 1:
        temp = temp.split('T')[1].split('.')[0]
    return(datetime.strptime(temp, "%H:%M:%S"))

def _time_at(msgs, str):
    msg = _find_msg(msgs, str)
    return(_extract_time(msg))

def compute_deployment_time(data):
    deployment_time = _time_at(data, 'finish deployment') - \
                      _time_at(data, 'start deployment')

    processing_time = _time_at(data, 'finish processing') - \
                      _time_at(data, 'start processing')

    pp([deployment_time, processing_time])

def compute_download_time(data):
    download_time = _time_at(data, 'download') - \
                    _time_at(data, 'start download')
    return download_time


def _find_msg(msgs, str):
    return(filter(lambda x:str in x, msgs))

#def compute_time_records(data):
  # hosts = list(set[h['host'] for h in data[0]])
 #  for h in hosts:
 #      msgs = [extract_msg(m) for m in mappers if m['host']== h]
#   for mapper in data
#       msg
#   deployment_time =
#   mappers_time =  _time_at(msgs[0], "start deployment")
#   delta_deployment  = _time_at(msgs[1], 'start upload') - _time_at(msgs[0], "start deployment")
#   return(str(delta_deployment.seconds))

def _get_service_offer(mapper, reducer):
    so_m = mapper[0]['_source']['fields']['service-offer']
    so_r = reducer[0]['_source']['fields']['service-offer']

    return [so_m,  so_r]


def get_product_info(data):
    raw_info = _find_msg(data, "finish downloading")
    info = raw_info[0].split('-')

    return(map(lambda x:x.strip(), info[3:5]))


def extract_field(data, field):

    return([v['_source'][field] for v in data.values()])



def _filter_field(hits, field, value):
    if hits['total'] > 0:
        hitsObj = hits['hits']
        result = [h for h in hitsObj \
                  if h['_source']['fields'][field] == value]
    else:
        result={}
    return result


def extract_node_data(run):
    mapper  = _filter_field(run, "nodename", "mapper")
    reducer = _filter_field(run, "nodename", "reducer")
    #pp(mapper)

    l = []
    for m in mapper:
        l.append((m['_source']['host'], m['_source']['message']))

    mappers = defaultdict(list)

    for v, k in l:
        mappers[v].append(k)


    pp(mappers[mappers.keys()[0]])
    compute_deployment_time(mappers[mappers.keys()[0]])

    return(mappers, reducer)


def query_run(duiid, cloud):
    query = {
         "query": {
             "bool": {
                 "must": [
                     {"match":{"fields.cloud" : cloud}},
                     {"match":{"fields.duiid" : duiid}}
                          ]
                      }
                    }
            }

    return res.search(index='_all', body=query, size=300)




def summarize_run(duiid, cloud):
    response = query_run(duiid, cloud)
    nodeData = extract_node_data(response['hits'])
 #       time_records = compute_time_records(nodeData)
 #       time     = compute_time_records(nodeData)
 #       product  = get_product_info(nodeData[0])


 #       run = { 'run_id': duiid,
  #              'cloud': cloud,
    #            'time': {'provisioning':,'install', 'deployment' }
 #               'time': time,
  #              'service-offer': _get_service_offer(nodeData[2],nodeData[3]),
  #              'product': product
 #             }
  #       return run


if __name__ ==  '__main__' :
    cloud = "eo-cesnet-cz1"
    duiid = "02592db8-fa44-4306-8fda-43a6dfe87bb3"
    response = query_run(duiid, cloud)
    run = summarize_run(duiid, cloud)

  # TODO: define complete run with raise error !
    pp(run)
