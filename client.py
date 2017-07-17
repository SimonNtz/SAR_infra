from threading import Thread
import requests
import json

def form_SLA(d={}):    
    d={'SLA':{}, 'RESP':{}}
    d['RESP']['sucess']='None'
    d['RESP']['URL']='None'
     
    print "input product list"
    d['SLA']['prd_list'] = raw_input()
    
    print "input time bound"
    d['SLA']['time'] = raw_input()
    
    print "input offer" 
    d['SLA']['offer'] = raw_input()  
    return(d)

def send_rqst(payload):
    url = "http://127.0.0.1:5000/SLA"
    requests.post(url, json.dumps(payload))

if __name__ == '__main__':
     _rqst = form_SLA()
     #print _rqst
     send_rqst(_rqst)
