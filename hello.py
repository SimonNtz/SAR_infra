from flask import Flask
from flask import request
app = Flask(__name__)

@app.route('/SLA', methods=['GET', 'POST'])
def hello_world():
    if request.method == 'POST':
	print request.data
    else:
	print "PLease use POST Method"
    return "ok"
