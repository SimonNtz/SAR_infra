from elasticsearch import Elasticsearch
import requests
import sys
from datetime import datetime
from pprint import pprint as pp
from collections import defaultdict
from slipstream.api import Api

api = Api()
api.login('simon1992', '12mc0v2ee64o9')

server_host = 'localhost'
res = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def _extract_time(m):
    return(datetime.strptime(m, "%Y-%m-%d %H:%M:%S"))

def _time_at(msgs, str):
    msg = _find_msg(msgs, str)
    if len(msg.split(' - ')) > 1:
        time = msg.split(' - ')[1].strip()
    else:
        time = ''.join(msg.split(': ')[1].replace('T', ' '))[0:19]
    return(_extract_time(time))


def _total_time(reducer, duiid):
    start = _extract_time(api.get_deployment(duiid)[3][0:19])
    total_time = _time_at(reducer, "start upload") - start

    return total_time.seconds

def _start_time(duiid):
    temp =  api.get_deployment(duiid)[3][0:19]

    return _extract_time(temp)


def _intra_node_time(data, duiid):
    start = _start_time(duiid)
    provisioning_time = _time_at(data, "currently in Provisioning") - start
    install_time      = _time_at(data, "start deployment")    -   start

    deployment_time = _time_at(data, 'finish deployment') - \
                      _time_at(data, 'start deployment')

    processing_time = _time_at(data, 'finish processing') - \
                      _time_at(data, 'start processing')

    intra_time = [provisioning_time.seconds,
                  install_time.seconds,
                  deployment_time.seconds,
                  processing_time.seconds]
    return([sum(intra_time[1:3])] + intra_time)
   # return({'provisioning':provisioning_time.seconds,
   #         'install': install_time.seconds,
   #         'deployment': deployment_time.seconds,
   #         'processing': processing_time.seconds})


def compute_time_records(mappers, reducer, duiid):
    mappers_time = map(lambda x:_intra_node_time(x, duiid), mappers.values())
    for i,v in enumerate(mappers.values()):
        mappers_time[i].append(_download_time(v))
   #mappers_time[i]['download'] = _download_time(v)
    return({'mappers':mappers_time,'total': _total_time(reducer, duiid)})

def _download_time(data):
    download_time = _time_at(data, 'finish downloading') - \
               _time_at(data, 'start downloading')
    return download_time.seconds


def _find_msg(msgs, str):

    return(filter(lambda x:str in x, msgs)[0])


def _get_service_offer(mapper, reducer):
    so_m = str(mapper[0]['_source']['fields']['service-offer'])
    so_r = str(reducer[0]['_source']['fields']['service-offer'])

    return [so_m,  so_r]


def get_product_info(data):
    raw_info = _find_msg(data, "finish downloading")
    info = raw_info.split(' - ')

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

def div_node(run):
    mapper  = _filter_field(run, "nodename", "mapper")
    reducer = _filter_field(run, "nodename", "reducer")

    return(mapper, reducer)

def extract_node_data(mapper, reducer, duiid):

    l = []
    for m in mapper:
        l.append((m['_source']['host'], m['_source']['message']))

    mappers = defaultdict(list)
    for v, k in l:
        mappers[v].append(k)

    reducer = [r['_source']['message'] for r in reducer]

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



def create_index(duiid, cloud, time_records, products, serviceOffers):
    run = {'cloud': cloud,
          'time_records': time_records,
          'products'    : products,
          'components'   : [serviceOffers[0], serviceOffers[1]]
       }

    print time_records
    index = res.index(index='sar_app',
                     doc_type='run_log',
                     id=8,#duiid,
                     body=run)
    print index['created']
    pp(run)

def summarize_run(duiid, cloud):
    response = query_run(duiid, cloud)
    [mappers, reducer] = div_node(response['hits'])
    [mappersData, reducerData] = extract_node_data(mappers, reducer, duiid)

    time_records = compute_time_records(mappersData, reducerData, duiid)
    products = map(lambda x:get_product_info(x), mappersData.values())
    serviceOffers = _get_service_offer(mappers, reducer)

    rep = create_index(duiid, cloud, time_records, products, serviceOffers)
    return rep

if __name__ ==  '__main__' :
    cloud = "eo-cesnet-cz1"
    duiid = "3d371680-86d2-49fc-b3d0-2fd5289cb1af"
    response = query_run(duiid, cloud)
    run = summarize_run(duiid, cloud)


 # TODO: define complete run with raise error !
    pp(run)
