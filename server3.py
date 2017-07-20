from flask import Flask, url_for, request, Response, render_template
app = Flask(__name__)
from slipstream.api import Api
import so_access as sa
import json
api = Api()
api.login('simon1992', '12mc0v2ee64o9')
from pprint import pprint as pp

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
	   
	       
	   rank = sa.push_req3(conn_legit, specs)	
           status = 201
               
	else:
           status = 400
           data = "Empty file"
       
        for i in range(10):	
	    print(rank['serviceOffers'][i]['price:unitCost'], rank['serviceOffers'][i]['connector'])
	#pp(rank['serviceOffers'][0:9])
       
        so_obj=rank['serviceOffers'][0] 
        so_id = so_obj['id']
        so_conn = so_obj['connector']
        
        print so_id
        print so_conn

	deploy_id = api.deploy('EO_Sentinel_1/procSAR',
                    cloud={'ELK_server':'eo-cesnet-cz1', 'mapper': 'eo-cesnet-cz1', 'reducer':'eo-cesnet-cz1'}, 
                    parameters={'ELK_server': {'service-offer': 'service-offer/38a4d187-9730-4afa-8c1b-eadb885d973a'},
                                'mapper' : {'service-offer': 'service-offer/deb7eb81-0881-4dff-9407-6230687f8a42'},
                                'reducer': {'service-offer': 'service-offer/5a97bbd7-6959-4259-9b2c-30fefdb08322'}},
                                 tags='EOproc', keep_running='never')
	
        resp = Response("SLA accepted", status=201, mimetype='application/json')
        resp.headers['Link'] = 'http://sixsq.eoproc.com'
         
	return resp
	
if __name__ == '__main__':
    app.run(
	host="0.0.0.0",
        port=int("80")
)
