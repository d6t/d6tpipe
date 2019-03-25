Pull Files from Remote to Local
==============================================

What is a Pipe?
---------------------------------------------

To pull files, you connect to a data pipe. A data pipe lets you manage remote and local data files.

Connecting to Pipes
---------------------------------------------

Connecting to a pipe is straight forward. 

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()
    api.list_pipes() # show available pipes

    pipe = d6tpipe.Pipe(api, 'pipe-name') # connect to a pipe


Show remote files
---------------------------------------------

To show files in the remote storage, run ``pipe.scan_remote()``.

.. code-block:: python

    pipe = d6tpipe.Pipe(api, 'pipe-name') # connect to a pipe
    pipe.scan_remote() # show remote files


Pulling Files to Local
---------------------------------------------

Pulling files will download files from the remote data repo to the local data repo. Typically you have to write a lot of code to download files and sync remote data sources. With d6tstack you can sync pull with just a few lines of python. 

.. code-block:: python

    pipe = d6tpipe.pipe.Pipe(api, 'pipe-name')
    pipe.pull_preview() # preview
    pipe.pull() # execute

Your files are now stored locally in a central location and conveniently accessible. See :doc:`Accessing Pipe Files <../files>` to learn how to use files after you have pulled them.

Which files are pulled?
---------------------------------------------

Only files that you don't have or that were modified are downloaded. You can manually control which files are downloaded or force download individual files, see advanced topics.

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
    pipe.pull(include='*.csv',exclude='private*.xlsx') # apply file filters

    # other useful operations
    api.list_local_pipes() # list pipes pulled
    pipe.files() # show synced files
    pipe.scan_remote() # show files in remote
    pipe.scan_remote(sortby='modified_at') # sorted by modified date
    pipe.is_synced() # any changes?
    pipe.remove_orphans() # delete orphan files
    pipe.delete_files() # reset local repo


Using Multipe Pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you work with multiple data sources, you can connect to multiple pipes.

.. code-block:: python

    pipe2 = d6tpipe.Pipe(api, 'another-pipe-name') # connect to multiple 

    # todo: how to sync pipe1 files to pipe2?
