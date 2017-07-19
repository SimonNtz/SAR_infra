from flask import Flask, render_template, request, Response, url_for
app = Flask(__name__)
import so_access as sa
import json


@app.route('/')
def form():
#    return "Welcome to EO service API!"
    return render_template('form_submit.html')

app.route('/hello/', methods=['POST'])
def hello():
    name=request.form['yourname']
    email=request.form['youremail']
    return render_template('form_action.html', name=name, email=email)


@app.route('/SLA_UI/', methods=['POST'])
def sla_ui():
    prd_list=request.form['prd_list']
    sla_time=request.form['sla_time']
    sla_offer=request.form['sla_offer']

    print prd_list    
   # DATA LOCALIZATION
    specs = [ "resource:type='DATA'", "resource:platform='S3'"]
    data = sa.push_req2(specs, [prd_list])
   
    print prd_list
    return render_template('form_action.html', prd_list=prd_list, sla_time=sla_time, sla_offer=sla_offer,data=data)

@app.route('/SLA_CLI', methods=['GET', 'POST'])
def api_message():
    if request.method == 'POST':
        if request.data:
           print type(request.data.split())
           # DATA LOCALIZATION
    	   specs = [ "resource:type='DATA'", "resource:platform='S3'"] 
	   data = sa.push_req2(specs, request.data.split(',')) 
	   print data
           status = 201
            
	else:
           status = 400
           data = "Empty file"
         
	resp = Response(data, status=201, mimetype='application/json')
        resp.headers['Link'] = 'http://sixsq.eoproc.com'
        
	return resp
	
if __name__ == '__main__':
    app.run(
	   host="0.0.0.0",
        port=int("80")	
)
