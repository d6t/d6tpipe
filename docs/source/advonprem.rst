Advanced Onprem
==============================================

Deploy your own onprem REST API
------------------------------

Instead of using the cloud API, you can deploy your own REST API which will store all the credentials in your own environment. You can either use the simple Flask app that ships with d6tpipe or you can write your own. Obviously with an onprem solution you won't be able to share data across organizations and take advantage of databolt protocols.

To the use d6tpipe Flask REST API follow the steps below. The files are available at https://github.com/d6t/d6tpipe/tree/master/flask

.. code-block:: python

    # runflaskapi.py
    from d6tpipe.utils.flaskapi import flaskapi
    from flask import Flask
    import os

    os.environ['D6TFLASKTOKEN']='securetoken'
    app = Flask(__name__)
    app.register_blueprint(flaskapi)
    if __name__ == "__main__":
        app.run(host='0.0.0.0', debug=True)

To deploy the Flask app.

.. code-block:: bash

    pip install circus

    # circusd.ini
    [watcher:flaskapi]
    cmd = python runflaskapi.py
    copy_env = True 

    circusd circusd.ini # to test
    circusd --daemon circusd.ini # to deploy
    circusctl restart flaskapi
    circusctl quit

Once your server is up and running, create a new profile that uses your onprem REST API.

.. code-block:: python

    import d6tpipe

    d6tpipe.api.ConfigManager(profile='onprem').init({'server':'http://yourip:port'})
    d6tpipe.api.ConfigManager(profile='onprem').update({'token':'securetoken'})
    api = d6tpipe.api.APIClient(profile='onprem')



