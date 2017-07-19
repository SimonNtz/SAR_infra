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
    
    prd_list = ['S1A_IW_GRDH_1SDV_20151226T182813_20151226T182838_009217_00D48F_5D5F',
                'S1A_IW_GRDH_1SDV_20160424T182813_20160424T182838_010967_010769_AA98',
                'S1A_IW_GRDH_1SDV_20160518T182817_20160518T182842_011317_011291_936E',
                'S1A_IW_GRDH_1SDV_20160611T182819_20160611T182844_011667_011DC0_391B',
                'S1A_IW_GRDH_1SDV_20160705T182820_20160705T182845_012017_0128E1_D4EE',
                'S1A_IW_GRDH_1SDV_20160729T182822_20160729T182847_012367_013456_E8BF',
                'S1A_IW_GRDH_1SDV_20160822T182823_20160822T182848_012717_013FFE_90AF',
                'S1A_IW_GRDH_1SDV_20160915T182824_20160915T182849_013067_014B77_1FCD'] 
    
     #_rqst = form_SLA()
     #print _rqst
    send_rqst({'prd_list':prd_list})
