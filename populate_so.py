from slipstream.api import Api
from pprint import pprint as pp
from xml.etree import ElementTree
import os, re, sys, math, requests, string

api = Api()
api.login('simon1992', '12mc0v2ee64o9')

# INPUT ARGS FORMAT : ( "host_url", "bucket_name")
# MANUAL INPUTS
connectors = { 'sos.exo.io' : 'exoscale-ch-gva',
               's3-eu-west-1.amazonaws.com' : 'ec2-eu-west'
}

def ls_bucket(host, bucket):
     response 	= requests.get(host + '/' + bucket)
     tree     	= ElementTree.fromstring(response.content)
     regex    	= re.compile('S1(.+?)SAFE')
     host_name  = re.match(r"https://(.*)", host).group(0)[8:]
     prd_list 	= []
     prd_dict  	= {}

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
     #prd_dict 	= {(c[0].text).split('/')[0] for c in tree if len(c) > 0 and re.search(regex, c[0].text)}
     #prd_dict  = {(c[0].text).split('/')[0] : c[3].text for c in tree
#				if len(c) > 0 and re.search(regex, c[0].text)}
#     for child in tree:
#         if len(child) > 0 and  re.search(regex, child[0].text):
#                prd_key = (child[0].text).split('/')[0]
#                prd_dict[prd_key] += child[0].Size
#		#prd_list.append((child[0].text).split('/')[0])
#     #prd_list = set(prd_list)
     return(prd_dict)

def build_so(prd_info):
    # Assign all info recovered from S3
    prd = {
	"connector" : {
		"href" : prd_info[1]['conn']},
	"name" : "SENTINEL-1 data product",
	"resource:platform" : "S3",
	"acl" : {"owner" : {"type" : "ROLE","principal" : "ADMIN"},
		 "rules" : [ {"principal" : "USER","right" : "VIEW","type" : "ROLE"},
			{"principal" : "ADMIN","right" : "ALL","type" : "ROLE"} ]},
	"resourceURI" : "http://sixsq.com/slipstream/1/ServiceOffer",
	"resource:class" : prd_info[0],
	"resource:type" : "DATA",
	"resource:disk" : prd_info[1]['size'],
	"resource:bucket": prd_info[1]['bucket'],
        "resource:host": prd_info[1]['host']
    }
    #pp(prd)
    api.cimi_add("serviceOffers", prd)

if __name__ == '__main__':
     d = ls_bucket(sys.argv[1], sys.argv[2]) # args: host, bucket
     map(build_so, d.items())
