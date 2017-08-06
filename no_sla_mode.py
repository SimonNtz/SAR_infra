from elasticsearch import Elasticsearch
from pprint import pprint as pp
import requests
import sys
from slipstream.api import Api
import lib_access as la
api = Api()
#api.login('simon1992', '12mc0v2ee64o9')


server_host = 'localhost'
res = Elasticsearch([{'host': 'localhost', 'port': 9200}])

min_specs = specs_ex = ["resource:vcpu='2'",
                     "resource:ram>'4000'",
                     "resource:disk>'49'",
                     "resource:operatingSystem='linux'"]

prod_list = ['S1A_IW_GRDH_1SDV_20151226T182813_20151226T182838_009217_00D48F_5D5F']


def _find_minimal_vm(cloud):
  cloud = "connector/href='%s'" % cloud
  return(la.request_vm(min_specs, cloud))

def deploy_benchmarking(cloud):
  min_vm = _find_minimal_vm(cloud)['serviceOffers'][0]['id']
  pp(min_vm)

  deploy_id = api.deploy('EO_Sentinel_1/procSAR',
                    cloud={'mapper': cloud, 'reducer':cloud},
                    parameters={'mapper' : {'service-offer': \
                                 min_vm, 'product-list':prod_list},
                                'reducer': {'service-offer': \
                                min_vm}},
                    tags='EOproc', keep_running='never')

  return(deploy_id)



if __name__ == '__main__':

    create_BDB( 'http://localhost:9200', 'ec2-eu-west', 'EOproc' 'sar6')
