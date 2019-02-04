from d6tpipe.utils.flaskapi import flaskapi
from flask import Flask
import os

os.environ['D6TFLASKTOKEN']='securetoken'
app = Flask(__name__)
app.register_blueprint(flaskapi, url_prefix='/v1/api')
if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)


'''
pip install circus

# circusd.ini
[watcher:flaskapi]
cmd = python runflaskapi.py
copy_env = True 

circusd circusd.ini # to test
circusd --daemon circusd.ini # to deploy
circusctl restart flaskapi
circusctl quit

'''