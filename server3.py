from flask import Flask, url_for, request, Response, render_template
import os, time, json, boto, boto.s3.connection, operator
from pprint import pprint as pp
from slipstream.api import Api
from datetime import datetime
from threading import Thread
import lib_access as la
import no_sla_mode as nsm
import summarizer_final as sumarizer
import numpy as np
# -*- coding: utf-8 -*-
app = Flask(__name__)
api = Api()


@app.route('/')
def form():
    return render_template('form_submit.html')


def connect_s3():
    access_key = "EXOb02927b095f5f60382e5513e"
    secret_key = "vL3PGh4fiPBNb5L4QNIatRyy2xSV8JkfCiCIum_dZJA"
    host       = "sos.exo.io"

    conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=host,
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
    return(conn)


def get_BDB(case):

    specs_ex = ["resource:vcpu='4'",
                       "resource:ram>'15000'",
                       "resource:disk>'100'",
                       "resource:operatingSystem='linux'"]
    specs_ex2 = ["resource:vcpu='4'",
                       "resource:ram>'15000'",
                       "resource:disk>'100'",
                       "resource:operatingSystem='linux'"]
    BDB = {

     'case1': {'run_a': ['c1',
                'service-offer/2d225047-9068-4555-81dd-d288562a57b1',
                500],
                     'run_b': ['c2',
                'service-offer/a264258f-b6fe-4f3e-b9f4-3722b6a1c6c7',
                 500],
                 'run_c': ['c2',
                 'service-offer/a264258f-b6fe-4f3e-b9f4-3722b6a1c6c7',
                 600],
                'run_d': ['c1',
                'service-offer/a264258f-b6fe-4f3e-b9f4-3722b6a1c6c7',
                650],
                'run_d': ['c4',
                'service-offer/a264258f-b6fe-4f3e-b9f4-3722b6a1c6c7',
                650]},

    'case2' : {},
    }
    return (BDB[case])


def apply_time_filter(BDB, t):
    return({k:v for k,v in BDB.iteritems() if v[2] <= t})


def apply_cloud_filter(BDB, c):
    return({k:v for k,v in BDB.iteritems() if v[0] in c})


def apply_filter_BDB(BDB, c, t):
    return(apply_time_filter(apply_cloud_filter(BDB,c),t))


def get_price(id):
    return(api.cimi_get(id).json['price:unitCost'])


def get_vm_specs(id):
    json = api.cimi_get(id).json
    spec_keys = ['id',
                 'resource:vcpu',
                 'resource:ram',
                 'resource:disk']
                 #'resource:typeDisk'] Maybe SSD boost the process
    return(tuple(v for k,v in json.items() if k in spec_keys))


def rank_per_price_BDB(BDB):
    temp = [(v[1], get_price(v[1])) for k,v in BDB.items()]
    temp.sort(key=lambda tuple: tuple[1])
    return(temp)


def rank_per_resource(list_id_res):
    list_id_res.sort(key=lambda tuple: tuple[1])
    return(temp[0])


def check_vm_specs(vm_ids):
    print "CHECK SPECS"
    vm_specs   = map(get_vm_specs, vm_ids)
    return(compare_vm_specs(vm_specs))


def compare_vm_specs(vm_specs):

    dtype = 'i8, i8, |S64 , i8'
    vm_specs = np.array(vm_specs, dtype=dtype)


    return(vm_specs[0][2])


def choose_vm(vm_set):
    best_price = vm_set[0][1]
    best_vms   = [k for k,v in vm_set if v == best_price]

    if len(best_vms) > 1:
        my_vm = check_vm_specs(best_vms)
    else:
        my_vm = best_vms[0]
    return my_vm


def DMM(clouds, time):
    """
    Lookup for runs in benchmarking DB which :

        - were deployed on the clouds where the data is located

        - have an execution equal or smaller than the SLA

        - The cheapest

        - with the best specs

    """
    BDB = get_BDB('case1')

    BDB_temp = apply_filter_BDB(BDB, clouds, 500 )

    vm_set   = rank_per_price_BDB(BDB_temp)

    my_vm    = choose_vm(vm_set)

    return(my_vm)


def download_product(bucket_id, conn, output_id):
    """
    :param   bucket_id: uri of the bucket
    :type    bucket_id: str

    :param   conn: interface to s3 bucket
    :type    conn: boto connect_s3 object

    param    output_id: product id
    type     output_id: str
    """

    bucket      = conn.get_bucket(bucket_id)
    key         = bucket.get_key(output_id)
    output_path = os.getcwd() + output_id
    key.get_contents_to_filename(output_path)

    print "Product stored @ %s." % output_id


def cancel_deployment(deployment_id):
    api.terminate(deployment_id)
    state = api.get_deployment(deployment_id)[2]
    while state != 'cancelled':
        print "Terminating deployment %s." % deployment_id
        time.sleep(5)
        api.terminate(deployment_id)


def watch_execution_time(start_time):
    time_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    delta = datetime.utcnow() - datetime.strptime(start_time,
                                            time_format)
    execution_time = divmod(delta.days * 86400 + delta.seconds, 60)
    return(execution_time)


def wait_product(deployment_id, time_limit):
    """
    :param   deployment_id: uuid of the deployment
    :type    deployment_id: str
    """
    deployment_data = api.get_deployment(deployment_id)
    state           = deployment_data[2]
    output_id       =  ""

    while state != "ready" and  not output_id:
        deployment_data = api.get_deployment(deployment_id)
        t = watch_execution_time(deployment_data[3])
        print "Waiting state ready. Currently in state : \
                 %s Time elapsed: %s mins, seconds" % (state, t)

        if (t[0]* 60 + t[1]) >= time_limit:
            cancel_deployment(deployment_id)
            return("SLA time bound exceeded. Deployment is cancelled.")

        time.sleep(45)
        state = deployment_data[2]
        output_id = deployment_data[8].split('/')[-1]

    conn = connect_s3()
    download_product("eodata_output2", conn, output_id)
    summarizer.

    return("Product %s delivered!" % outpud_id)


def _all_products_on_cloud(c, rep_so, prod_list):
    products_cloud = ['x' for so in rep_so if so['connector']['href'] == c]
    return len(products_cloud) == len(prod_list)


def find_data_loc(prod_list):
    """
    :param   cloud_set: Input product list
    :type    cloud_set: list

    :param   cloud_set: response fron service catalog for data localization
    :type    cloud_set: dictionnary
    """
    # Not needed but speed the lookup up on the db
    specs_data         = ["resource:type='DATA'", "resource:platform='S3'"]
    #rep_so             = la.request_data(specs_data, prod_list)['serviceOffers']
    # push_req2 should be mocked in Unittesting

    #cloud_set      = list(set([c['connector']['href'] for c in rep_so]))
    cloud_set      = []

    #['cloud_a, cloud_b, cloud_c', 'cloud_d']
    #cloud_legit    = []
    cloud_legit    = ['c1', 'c2', 'c3', 'c4'] # FAKED

    # for c in cloud_set:
    #     if _all_products_on_cloud(c, rep_so, prod_list):
    #          cloud_legit.append("connector/href='%s'" % c)

    return(cloud_legit)


def _schema_validation(jsonData):
    """
    Input data Schema:
    - A JSON with top hierarchy 'SLA' and 'results' dicts:

    jsonData = {'SLA':dict, 'result':dict}

    dict('SLA')    = {'requirements':['time','price', 'resolution'], 'order':['prod_list']}
    dict('result') = {''}
    """
    if not "SLA" in jsonData:
        raise ValueError("No 'SLA' in given data")
    if not "result" in jsonData:
        raise ValueError("No 'result' in given data")
    for k,v in jsonData.items():
        if not isinstance(v, dict):
            raise ValueError("%s is not a dict in given data" % k)

    SLA = jsonData['SLA']

    if not "product_list" in SLA:
        raise ValueError("Missing product list in given SLA data")
    if not "requirements" in SLA:
        raise ValueError("Missing requirements in given SLA data")

    for k,v in jsonData['SLA'].items():
        if not isinstance(v, list):
            raise ValueError("%s is not a list in given data" % k)

    return True

def check_BDB(clouds):
    index='/sar'
    type='/offer-cloud'
    host = 'http://localhost:9200'
    req_index = request.get(host + index)

    deploy_id = []
    for c in clouds:
        req_id    = request.get(host + index + type + id)

        if req_index != '<Response [200]>':
            deploy_id = (c, nsm.create_BDB(host, c, type index))
        elif req_id != '<Response [200]>':
            deploy_id = (c, nsm.create_BDB(host, c, type))

        watch_execution_time(deploy_id, 9999)
        print c + "benchmark done."

    return "benchmark db created"

def _request_validation(request):
    if request.method == 'POST':
        _schema_validation(request.get_json())
    else:
        raise ValueError("Not a POST request")


@app.route('/SLA_TEST', methods=['POST'])
def sla_test():
    print request.get_json()
    return "check"


@app.route('/SLA_CLI', methods=['POST'])
def sla_cli():
# Schema on Input
# Validation
    try:
        _request_validation(request)
        data = request.get_json()
        _sla = data['SLA']
        pp(_sla)

        prod_list  =  _sla['product_list']
        data_loc   = find_data_loc(prod_list)
        print "Data located in: %s" % data_loc
        check_BDB(clouds)


        msg    = ""
        status = ""

        if data_loc:
            msg    = "SLA accepted! "
            status = "201"
            time = 500
            best_so = DMM(data_loc, time)
            print best_so
            #best_so = la.request_vm(specs_vm, data_loc)['serviceOffers'][0]
            so_id   = 'eo-cesnet-cz1' # best_obj['connector']
            so_conn = 'service-offer/deb7eb81-0881-4dff-9407-6230687f8a42' # best_obj['id']
            print so_id
            print so_conn

            deploy_id = api.deploy('EO_Sentinel_1/procSAR',
                        cloud={'mapper': so_id, 'reducer':'eo-cesnet-cz1'},
                        parameters={'mapper' : {'service-offer': \
                                     so_conn, 'product-list':prod_list},
                                    'reducer': {'service-offer': \
                                    'service-offer/5a97bbd7-6959-4259-9b2c-30fefdb08322'}},
                        tags='EOproc', keep_running='never')

            daemon_watcher = Thread(target = wait_product, args = (deploy_id, time))
            daemon_watcher.setDaemon(True)
            daemon_watcher.start()
        else:
            msg = "Data not found in clouds!"
            status = 412


    except ValueError as err:
        msg = "Value error: {0} ".format(err)
        status = "404"
        print("Value error: {0} ".format(err))

    resp = Response(msg, status=status, mimetype='application/json')
    resp.headers['Link'] = 'http://sixsq.eoproc.com'
    return resp

if __name__ == '__main__':
    api.login('simon1992', '12mc0v2ee64o9')
    app.run(
        host="0.0.0.0",
        port=int("80")
)
