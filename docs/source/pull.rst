Pull Files
==============================================

What is a Pipe?
---------------------------------------------

To pull files you need to connect to a pipe. A pipe is the main object that will interact with. It allows you to pull, push and access files. 

Connecting to Pipes
---------------------------------------------

Connecting to a pipe is straight forward. 

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()

    pipe = d6tpipe.Pipe(api, 'pipe-name') # connect to a pipe
    pipe.scan_remote_filenames() # show files in remote


Pulling Files
---------------------------------------------

Typically you have to write a lot of code to download files and sync pipe data sources. With d6tstack you can sync pull with just a few lines of python. 

.. code-block:: python

    pipe = d6tpipe.pipe.Pipe(api, 'pipe-name')
    pipe.pull_preview() # preview
    pipe.pull() # execute

Your files are now stored locally in a central location and conveniently accessible. See :doc:`Accessing Pipe Files <../files>` to learn how to use files after you have pulled them.

Advanced Topics
---------------------------------------------

Pull Modes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can control which files are pulled/pushed

* ``default``: modified and new files  
* ``new``: new files only  
* ``mod``: modified files only  
* ``all``: all files, good for resetting a pipe  

.. code-block:: python

    pipe = d6tpipe.pipe.Pipe(api, 'test', mode='all') # set mode
    pipe.pull() # pull all files
    pipe.setmode('all') # dynamically changing mode


Useful Pipe Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below is a list of useful functions. See the reference :ref:`modindex` for details.

.. code-block:: python

    # advanced pull options
    pipe.pull(['a.csv']) # force pull on selected files
    pipe.pull(include='*.csv',exclude=['backup*.csv','oldstuff/*.csv']) # apply file filters

    # other useful operations
    api.list_local_pipes() # list pipes pulled
    pipe = d6tpipe.pipe.Pipe(api, 'test', sortby='modified_at') # sort files by mod date
    pipe.list_remote() # show files in remote
    pipe.list_remote(sortby='modified_at') # sorted by modified date
    pipe.is_synced() # any changes?
    pipe.remove_orphans() # delete orphan files
    pipe.delete_all_local() # reset local repo
    pipe.dbfiles.all() # inspect local files db


Using Multipe Pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you work with multiple data sources, you can connect to multiple pipes.

.. code-block:: python

    pipe2 = d6tpipe.Pipe(api, 'another-pipe-name') # connect to multiple pipes


Setting Proxy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are behind a proxy, you may have to set your proxy to pull files.

.. code-block:: python

    import os
    cfg_proxy = "http://yourip:port"
    os.environ["http_proxy"] = cfg_proxy; os.environ["HTTP_PROXY"] = cfg_proxy;
    os.environ["https_proxy"] = cfg_proxy; os.environ["HTTPS_PROXY"] = cfg_proxy;

