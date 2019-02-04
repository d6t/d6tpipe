Connect to repo API
==============================================

What is the cloud repo API?
------------------------------

The repo API stores details about remotes and pipes and is the first object you need to instantiate. There are alternatives to the cloud API, covered in advanced sections, but the cloud repo API is the best way to get started.

Register and login
------------------------------

To use the cloud repo API, you need to register which you can do from inside python.

.. code-block:: python

    api = d6tpipe.api.APIClient() # you probably get a warning message
    api.register('your-username','your@email.com','password') # do this just once

In the future, in case you need to log in to d6tpipe on another machine after registering, call `login()` instead of `register()`.

.. code-block:: python

    # log in on another machine
    api = d6tpipe.api.APIClient()
    api.login('your-username','password') # do this just once

Now that you are registered you can :doc:`pull files <../pull>`.


Advanced Topics
---------------------------------------------

Managing your cloud API token
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run the below if you forgot your token or need to reset. 

.. code-block:: python

    # retrieve token
    api.forgotToken('your-username','password')

    # reset token
    api = d6tpipe.api.APIClient(token=None)
    api.register('your-username','your@email.com','password')


Local Mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The best way to get started is to use the cloud API. As an alternative to the cloud API, you can use d6tpipe in local mode which stores all remote and pipe details locally. That is fast and secure, but limits functionality.  

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APILocal() # local mode

Local vs Server Mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The local mode stores everything locally so nothing ends up in the cloud. While that is fast and secure, it limits functionality. The server can:  

* share remotes and pipes across teams and organizations
* remotely scan for file changes and centrally cache results
* regularly check for file changes on a schedule
* remotely mirror datasets, eg from ftp to S3

The way you interface with d6tpipe is pretty much identical between the two modes so you can easily switch between them. So you can start in local mode and then switch to server mode to take advantage of advanced features.

Onprem repo API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can deploy an onprem repo API, contact <support@databolt.tech> for details.
