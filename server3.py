from flask import Flask, url_for, request, Response, render_template
app = Flask(__name__)
from slipstream.api import Api
import so_access as sa
import json
api = Api()
api.login('simon1992', '12mc0v2ee64o9')
from pprint import pprint as pp
import boto
import boto.s3.connection
import time
from threading import Thread
from datetime import datetime
@app.route('/')
def form():
    return render_template('form_submit.html')

@app.route('/SLA_UI/', methods=['POST'])
def sla_ui():
     prod_list=request.form['prod_list']
     sla_offer=request.form['sla_offer']
     sla_time=request.form['sla_time']
   # DATA LOCALIZATION
     specs = [ "resource:type='DATA'", "resource:platform='S3'"]
     data = sa.push_req2(specs, prod_list.split(','))
     return render_template('form_action.html', 
			     prod_list=prod_list, 
                             data=data, 
                             sla_offer=sla_offer, 
                                sla_time=sla_time)


def deploy(req, p_l):
   if req['count']>0:
	connectors = req['conectors']


def connect_s3():
    access_key = "EXOb02927b095f5f60382e5513e"
    secret_key = "vL3PGh4fiPBNb5L4QNIatRyy2xSV8JkfCiCIum_dZJA"
    host = "sos.exo.io"     
    
    conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = host,
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
    return(conn)

def download_product(bucket_id, conn, output_id):
     bucket = conn.get_bucket(bucket_id)
     key    = bucket.get_key(output_id)
     key.get_contents_to_filename(output_id)
     print "Product %s saved" %output_id

def wait_product(deployment_id):
    state     = api.get_deployment(deployment_id)[2] 
    start_time = api.get_deployment(deployment_id)[3] 
    output_id = api.get_deployment(deployment_id)[8] # .split('/')[-1]
    time_format = '%Y-%m-%d %H:%M:%S.%f UTC'  
    while state != "ready" and  not output_id:
        delta = datetime.strptime(datetime.utcnow().strftime(time_format), time_format) - datetime.strptime(start_time, time_format) 
        print "waiting state ready. Currently in state %s " % state #started at %s 
					#time elapsed %s min,seconds" %  
					#(state, start_time, divmod(delta.days * 86400 + delta.seconds, 60))
        
	time.sleep(45)
	state = api.get_deployment(deployment_id)[2]
    
    output_id = api.get_deployment(deployment_id)[8] # .split('/' 
    conn = connect_s3()     
    download_product("eodata_output2", conn, output_id)
    return("Job done")
@app.route('/SLA_CLI', methods=['GET', 'POST'])
def sla_cli():
    if request.method == 'POST':
        if request.data:
           
           # DATA LOCALIZATION
	   prod_list =  request.data.split(',')
          
    	   specs = [ "resource:type='DATA'", "resource:platform='S3'"] 
	   data = sa.push_req2(specs, prod_list) 
	   print data
           
           # PRICE RANKING
           
           specs = ["resource:vcpu='4'", "resource:ram>'15000'","resource:disk>'100'", "resource:operatingSystem='linux'"]
            
           rep = data['serviceOffers']
           conn_set = list(set([c['connector']['href'] for c in rep]))
           print conn_set
           conn_legit = []   
	   for c in conn_set:	
	       if len([ 'x' for so in rep if so['connector']['href'] == c]) == len(prod_list): 
	           conn_legit.append("connector/href='%s'" %c)
           rank = []
           msg=""
           status= 0
           print len(conn_legit)           
           if len(conn_legit)==0:  
	       status = 412
               msg = "Data not found in clouds! " 
	       rank = []	
           else :       
	       rank = sa.push_req3(conn_legit, specs)	
               status = 201

               so_obj=rank['serviceOffers'][0] 
               so_id = so_obj['id']
               so_conn = so_obj['connector']
        
               print so_id
               print so_conn

	       deploy_id = api.deploy('EO_Sentinel_1/procSAR',
                    cloud={'ELK_server':'eo-cesnet-cz1', 'mapper': 'eo-cesnet-cz1', 'reducer':'eo-cesnet-cz1'}, 
                    parameters={'ELK_server': {'service-offer': 'service-offer/38a4d187-9730-4afa-8c1b-eadb885d973a'},
                                'mapper' : {'service-offer': 'service-offer/deb7eb81-0881-4dff-9407-6230687f8a42', 'product-list':prod_list},
                                'reducer': {'service-offer': 'service-offer/5a97bbd7-6959-4259-9b2c-30fefdb08322'}},
                                 tags='EOproc', keep_running='always')
	       Thread(target = wait_product, args = (deploy_id,)).start()
               msg="SLA accepted"
       
        #for i in range(10):	
	#    print(rank['serviceOffers'][i]['price:unitCost'], rank['serviceOffers'][i]['connector'])
	#pp(rank['serviceOffers'][0:9])
       
        resp = Response(msg, status=status, mimetype='application/json')
        resp.headers['Link'] = 'http://sixsq.eoproc.com' 
       
	return resp
	
if __name__ == '__main__':
    app.run(
	host="0.0.0.0",
        port=int("80")
)
