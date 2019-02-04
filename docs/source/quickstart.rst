Quickstart
==============================================

First-time setup and registration
--------------------------------------

.. code-block:: python
    
    import d6tpipe
    d6tpipe.api.ConfigManager().init() # just once

    # register with cloud repo API
    api = d6tpipe.api.APIClient() # you likely get a messaging asking to register
    api.register('your-username','your@email.com','password') # just once

All set! In the future, you may need to log in to d6tpipe on another machine after you already registered, call `login()` instead of `register()`.

.. code-block:: python

    # to log in on another machine
    api = d6tpipe.api.APIClient()
    api.login('your-username','password') # just once


See :doc:`Config <../config>` and :doc:`Connect <../connect>` for details. 

Pull files
------------------------------

To pull files, you need to connect to a data pipe. The data pipe allows you to manage the data files.

.. code-block:: python
    
    import d6tpipe
    api = d6tpipe.api.APIClient()

    # list data pipes you have access to
    api.list_pipes()

    # pull files from a data pipe
    pipe = d6tpipe.Pipe(api, 'intro-stat-learning') # connect to a data pipe
    pipe.pull_preview() # preview files and size
    pipe.pull() # download all data with just one command

See :doc:`Pull Files<../pull>` for details.


Access and read files
------------------------------

.. code-block:: python
    
    import d6tpipe
    api = d6tpipe.api.APIClient()
    pipe = d6tpipe.Pipe(api, 'intro-stat-learning') # connect to a data pipe
    pipe.pull()

    # list files in pipe
    print(pipe.filenames())

    # read a file into pandas
    import pandas as pd
    df = pd.read_csv(pipe.dirpath/'Advertising.csv') 
    print(df.head())

See :doc:`Read Files <../read>` for details.

Process files
------------------------------

.. code-block:: python

    # use readParams to quickly load data
    df = pd.read_csv(pipe.dirpath / 'Advertising.csv', **pipe.readparams['pandas'])
    print(df.head())

    # read multiple files into dask
    import dask.dataframe as dd
    files = pipe.files(include='Advertising*.csv')
    ddf = dd.read_csv(files, **pipe.readparams['dask'])
    print(ddf.head())

    # open most recent CSV
    df = pd.read_csv(pipe.files(include='*.csv')[-1])

    # save data to pipe
    df.to_csv(pipe.dirpath/'new.csv')

See :doc:`Process Files <../read>` for details.

Advanced Topics
---------------------------------------------

This covers pushing files and creating your own remote file storage and data pipes.

Write and Push Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have been given write access or have your own pipes, you can also push files.

.. code-block:: python

    import d6tpipe
    import pandas as pd
    api = d6tpipe.api.APIClient()
    pipe = d6tpipe.pipe.Pipe(api, 'intro-stat-learning')
    df = pd.read_csv(pipe.dirpath / 'Advertising.csv', **pipe.readparams['pandas'])
    
    # conveniently process and save files in a central repo
    import sklearn.preprocessing
    df_scaled = df.apply(lambda x: sklearn.preprocessing.scale(x))
    df_scaled.to_csv(pipe.dirpath/'Advertising-scaled.csv') # pipe.dirpath points to local pipe folder

    # alternatively, import another folder
    pipe.import_dir('/some/folder')

    # list files in local directory
    print(pipe.scan_local_filenames())

    # upload files - just one command!
    pipe.push_preview() # preview files and size
    pipe.push() # execute

See :doc:`Push <../push>` for details.

Create remotes and pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You might want to create your own remote file storage that you control. A remote refers to the the remote file storage and a pipe is a connection to that file storage. You can have multiple pipes to the same remote.

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()
    
    # managed file stores can be created quickly with just one command 
    d6tpipe.api.create_pipe_with_remote(api, {'name': 'your-data-files', 'protocol': 'd6tfree'})

See :doc:`Remotes <../remotes>` and :doc:`Pipes <../pipes>` for details. For creating self-hosted remotes, see :doc:`Advanced Remotes <../advremotes>` .

Share data repo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After you've created a remote or pipe, you can manage access permissions. By default only you have access so to share it with others you have to grant them access.

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()

    # give another user access
    settings = {"user":"another-user","role":"read"} # read, write, admin
    d6tpipe.create_or_update_permissions(api, 'your-remote', settings)

    # make data repo public
    settings = {"user":"public","role":"read"}
    d6tpipe.create_or_update_permissions(api, 'your-remote', settings)

See :doc:`Remotes <../remotes>` for details.

