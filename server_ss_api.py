from flask import Flask
from flask import request
from slipstream.api import Api
from threading import Thread
from pprint import pprint as pp
import thread
import Queue
import so_access

app = Flask(__name__)
api = Api()
api.login('simon1992', '12mc0v2ee64o9')

def question_service_offer(module, clouds, comp, queue):
    # Get the service offer tempate
    json={'moduleUri': module,
         'userConnectors': clouds}
    # Get the JSON template for Service Offer
    r = api.session.put(api.endpoint+'/ui/placement', json=json)
    so_temp = r.json()
    pp(so_temp)

    # Find the components index of interest
    comp_idx = {}
    for c in comp:
        comp_idx[c] = [ind for ind, node in enumerate(so_temp['components']) if node[u'node']== c][0]

    # Insert component specs
    for c in comp:
        for key,value in comp[c].items():
            so_temp[u'components'][comp_idx[c]][key] = value

    # Get Service Offers response
    r = api.session.put(api.endpoint+'/filter-rank', json=so_temp)

    raw_so = r.json()
    pp(raw_so)
    resp = []
    for c in comp:
    #SEE if we don't keep all offers here we take the cheapest
        resp.append((c,
              raw_so['components'][comp_idx[c]]['connectors'][0]['id']))
    pp(resp)
    queue.put(resp)
    return "ok"

def deploy_rqst(module, queue):
    so_uri    = queue.get()
    cloud   = {}
    params  = {}

    for comp,so in so_uri:
        cloud[comp]  = api.cimi_get(so).connector
        params[comp] = {'service-offer':so}
    pp(cloud)
    pp(params)
    api.deploy(module, cloud=cloud, parameters=params,
	        tags='service_offer_test', keep_running='never')

#@app.route('/SLA', methods=['POST'])
def my_endpoint_handler():
    '''
    The SLA consists of a product order and a time restriction
    request.data' contains a JSON of the request.
    We start by questioning the DMM get the data localization (Clouds)
    and the instance t-shirt size. After that, we look for the upper closest match
    in the service offers. Finally, we deploy the module configured with the findings.
    '''

    # DATA LOCALIZATION
    specs = [ "resource:type='DATA'", "resource:platform='S3'"] 
       #so_access.push_req2(specs, request.data['prd_list'])
    print request.data



    # DMM gives what are the data localization and the optimal instance's t-shirt size
    comp = {u'mapper':{u'cpu.nb': u'4', u'disk.GB': u'100', u'ram.GB': u'12'},
             u'reducer':{u'cpu.nb': u'4', u'disk.GB': u'100', u'ram.GB': u'12'}}
    


  
    deploy_rqst("module/EO_Sentinel_1/procSAR/13158", queue)
    
   
    return "Thanks"

if __name__== '__main__':
    app.run(debug=True)
    my_endpoint_handler()
    #print "check"

    print_test("lol")

    # For local context in threads
    #def handle_sub_view(req):
    #    with app.test_request_context():
    #			['exoscale-ch-gva', 'ec2-eu-west-2'])
    #           # Do Expensive work with context of request if its big can be practicle
    #task.start_new_thread(handle_sub_view, (request,))
