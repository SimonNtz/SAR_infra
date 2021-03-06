'''
    This script runs a process which populates Slipstream
    service catalog with SENTINEL-1 data from S3 buckets.
'''
from slipstream.api import Api
from pprint import pprint as pp
from xml.etree import ElementTree
import os, re, sys, math, requests, string

# Connect to Nuvla account
api = Api()
api.login('<nuvla_login>', '<nuvla_password>')

# INPUT ARGS FORMAT : ( "host_url", "bucket_name")
# MANUAL INPUTS

connectors = { 'sos.exo.io' : 'exoscale-ch-gva',
               's3-eu-west-1.amazonaws.com' : 'ec2-eu-west'}

def ls_bucket(host, bucket):
    """
    :param      host: URL of S3 storage
    :type       host: str

    :param      bucket: bucket unique name
    :type       bucket: str
    """
    response 	= requests.get(host + '/' + bucket)
    tree     	= ElementTree.fromstring(response.content)
    regex    	= re.compile('S1(.+?)SAFE')
    host_name  = re.match(r"https://(.*)", host).group(0)[8:]
    prd_list 	= []
    prd_dict  	= {}

    # SCRAPPER OF FILES URL ON THE S3 PUBLIC XML FILE
    for c in tree:
	if len(c) > 0 and re.search(regex, c[0].text):
            c_key  = (c[0].text).split('/')[0]
            c_size = (c[3].text)
	    if c_key in prd_dict:
	        prd_dict[c_key]['size'] += int(c_size)
            else:
		prd_dict[c_key] ={}
                prd_dict[c_key]['size']   = int(c_size)
                prd_dict[c_key]['bucket'] = tree[0].text
                prd_dict[c_key]['host']   = host_name
                prd_dict[c_key]['conn']   = connectors[host_name]
     return(prd_dict)

def build_so(prod_info):
    """
    :param      prod_info: dictionnary containing all the attributes of a product
    :type       prod_info: dict
    """
    prd = {
	"connector" : {
		"href" : prod_info[1]['conn']},
	"name" : "SENTINEL-1 data product",
	"resource:platform" : "S3",
	"acl" : {"owner" : {"type" : "ROLE","principal" : "ADMIN"},
		 "rules" : [ {"principal" : "USER","right" : "VIEW","type" : "ROLE"},
			{"principal" : "ADMIN","right" : "ALL","type" : "ROLE"} ]},
	"resourceURI" : "http://sixsq.com/slipstream/1/ServiceOffer",
	"resource:class" : prod_info[0],
	"resource:type" : "DATA",
	"resource:disk" : prod_info[1]['size'],
	"resource:bucket": prod_info[1]['bucket'],
        "resource:host": prod_info[1]['host']
    }
    # POST NEW SERVICE OFFER
    api.cimi_add("serviceOffers", prd)

if __name__ == '__main__':
     d = ls_bucket(sys.argv[1], sys.argv[2]) # args: host, bucket
     map(build_so, d.items())
