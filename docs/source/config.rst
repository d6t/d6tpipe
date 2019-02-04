Configuring d6tpipe
==============================================

First-time setup
------------------------------

To quickly configure d6pipe with default settings, run the code below. With that you're good to :doc:`connect to the cloud repo API <../connect>`.

.. code-block:: python
    
    import d6tpipe
    d6tpipe.api.ConfigManager().init() # just once

Manage Local File Storage
------------------------------

d6tpipe stores all files in a central location to make it easy for you to reference data files across multiple projects. By default this is ``~/d6tpipe``. You can chage where files are stored.

.. code-block:: python
    
    # option 1: set custom path BEFORE init
    d6tpipe.api.ConfigManager().init({'filerepo':'/some/path/'})

    # option 2: move file repo AFTER init
    api = d6tpipe.api.APILocal()
    api.move_repo('/new/path')
    print(api.filerepo)

    # option 3: manually move file repo and update config
    d6tpipe.api.ConfigManager().update({'filerepo':'/some/path/'})

Advanced Topics
---------------------------------------------

Customize Init Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Optionally, you can pass in a config dict. Your options are:  

* ``filerepo``: path to where files are stored  
* ``server``: if you are not using local mode, URL to REST API server  
* ``token``: auth token for REST API server  
* ``key``: encryption key

.. code-block:: python
    
    # example: pass file repo path
    d6tpipe.api.ConfigManager().init({'filerepo':'/some/path/'})


Update an Existing Config 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can change config options by passing the settings you want to update.

.. code-block:: python
    
    # example: update REST token and username
    d6tpipe.api.ConfigManager().update({'token':token})
    d6tpipe.api.ConfigManager().update({'username':username})

NB: Don't use config update to change settings for remotes and pipes.


Using Multiple Profiles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

d6tpipe supports the use of profiles so you can use different settings.

.. code-block:: python
    
    # make profiles
    d6tpipe.api.ConfigManager(profile='user2').init()
    d6tpipe.api.ConfigManager(profile='projectA').init({'filerepo':'/some/path/'})
    d6tpipe.api.ConfigManager(profile='projectB').init({'filerepo':'/another/path/'})
    d6tpipe.api.ConfigManager(profile='cloud').init({'server':'http://api.databolt.tech'})
    d6tpipe.api.ConfigManager(profile='onprem').init({'server':'http://yourip'})

    # connect using a profile name
    api = d6tpipe.api.APIClient(profile='onprem')

    # show profiles
    api.profiles()

