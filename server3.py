from flask import Flask, url_for, request, Response, render_template
import os, time, json, boto, boto.s3.connection
from pprint import pprint as pp
from slipstream.api import Api
from datetime import datetime
from threading import Thread
import so_access as sa
import lib_access as la
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

def wait_product(deployment_id):
    """
    :param   deployment_id: uuid of the deployment
    :type    deployment_id: str
    """
    deployment_data = api.get_deployment(deployment_id)
    state           = deployment_data[2]
    output_id       =  ""

    while state != "ready" and  not output_id:
        delta = datetime.utcnow() - datetime.strptime(deployment_data[3], '%Y-%m-%d %H:%M:%S.%f UTC')
        print "Waiting state ready. Currently in state : %s Time elapsed: %s mins, seconds" % \
                            (state, divmod(delta.days * 86400 + delta.seconds, 60))
        time.sleep(45)
        deployment_data = api.get_deployment(deployment_id)
        state = deployment_data[2]
        output_id = deployment_data[8].split('/')[-1]

    conn = connect_s3()
    download_product("eodata_output2", conn, output_id)
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
    rep_so             = la.request_data(specs_data, prod_list)['serviceOffers']
    # push_req2 should be mocked in Unittesting

    cloud_set      = list(set([c['connector']['href'] for c in rep_so]))
    cloud_legit    = []

    for c in cloud_set:
        if _all_products_on_cloud(c, rep_so, prod_list):
             cloud_legit.append("connector/href='%s'" % c)

    return(cloud_legit)


def _schema_validation(jsonData):
    """
    Input data Schema:
    - A JSON with top hierarchy 'SLA' and 'results' dicts:

    jsonData = {'SLA':dict, 'result':dict}

    dict('SLA')    = {'requirements':['time',offer'], 'order':['prod_list']}
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



def _request_validation(request):
    if request.method == 'POST':
        _schema_validation(request.get_json())
    else:
        raise ValueError("Not a POST request")




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

        msg    = ""
        status = ""
        if data_loc:
            msg    = "SLA accepted! "
            status = "201"
            # Dummy instance size should be found from benchmarking
            specs_vm = ["resource:vcpu='4'",
                              "resource:ram>'15000'",
                              "resource:disk>'100'",
                              "resource:operatingSystem='linux'"]
            # The cheapest the best
            best_so = la.request_vm(specs_vm, data_loc)['serviceOffers'][0]
            so_id   = 'eo-cesnet-cz1' # best_obj['connector']
            so_conn = 'service-offer/deb7eb81-0881-4dff-9407-6230687f8a42' # best_obj['id']
            print so_id
            print so_conn

            deploy_id = api.deploy('EO_Sentinel_1/procSAR',
                        cloud={'mapper': so_id, 'reducer':'eo-cesnet-cz1'},
                        parameters={'mapper' : {'service-offer': so_conn, 'product-list':prod_list},
                                    'reducer': {'service-offer': 'service-offer/5a97bbd7-6959-4259-9b2c-30fefdb08322'}},
                        tags='EOproc', keep_running='never')

            daemon_watcher = Thread(target = wait_product, args = (deploy_id,))
            daemon_watcher.setDaemon(True)
            daemon_watcher.start()
        else:
            msg = "Data not found in clouds! "
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
