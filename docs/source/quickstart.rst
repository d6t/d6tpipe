Quickstart
==============================================

First-time setup and registration
--------------------------------------

1. Sign up at https://pipe.databolt.tech/gui/user-signup/
2. Get API token at http://pipe.databolt.tech/gui/api-access
3. Set up d6tpipe to use token 

.. code-block:: python
    
    import d6tpipe
    # cloud repo API token
    api = d6tpipe.api.APIClient()
    api.setToken('your-token') # DONT SHARE YOUR TOKEN! Do not save in code, just run it once

**Run this ONCE. You do NOT to have to run this every time you use d6tpipe**

See :doc:`Config <../config>` and :doc:`Connect <../connect>` for details. 

Pull files from remote to local
----------------------------------

To pull files, you connect to a data pipe. A data pipe lets you manage remote and local data files.

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


Access and read local files
------------------------------

Remote files that are pulled get stored in a central local file directory. 

.. code-block:: python
    
    # show local files
    pipe.dirpath # where files are stored
    pipe.files()

    # read a file into pandas
    import pandas as pd
    df = pd.read_csv(pipe.dirpath/'Advertising.csv') 
    print(df.head())

See :doc:`Read Files <../read>` for details.

Process files
------------------------------

You now have a lot of powerful functions to easily manage all your files from a central location across multiple projects.

.. code-block:: python

    # use schema to quickly load data
    df = pd.read_csv(pipe.dirpath / 'Advertising.csv', **pipe.schema['pandas'])
    print(df.head())

    # read multiple files into dask
    import dask.dataframe as dd
    files = pipe.filepaths(include='Advertising*.csv')
    ddf = dd.read_csv(files, **pipe.schema['dask'])
    print(ddf.head())

    # open most recent CSV
    df = pd.read_csv(pipe.files(sortby='mod')[-1])

    # save data to local files
    df.to_csv(pipe.dirpath/'new.csv')

See :doc:`Process Files <../read>` for details.

Advanced Topics
---------------------------------------------

This covers pushing files and creating your own remote file storage and data pipes.

Write Local Files and Push to Remote
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can easily save new files to the pipe. You can also push files from local to remote if you have write access or manage your own pipes.

.. code-block:: python
    
    # create some new data
    import sklearn.preprocessing
    df_scaled = df.apply(lambda x: sklearn.preprocessing.scale(x))

    # conveniently save files in a central repo
    df_scaled.to_csv(pipe.dirpath/'Advertising-scaled.csv') # pipe.dirpath points to local pipe folder

    # alternatively, import another folder
    pipe.import_dir('/some/folder/')

    # list files in local directory
    pipe.scan_local()

    # upload files - just one command!
    pipe.push_preview() # preview files and size
    pipe.push() # execute

See :doc:`Push <../push>` for details.

Register and administer pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can register your own pipes that point to your own remote data storage. d6tpipe has managed remotes which makes it very easy for you to set up and manage professional remote data file storage.

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()
    
    # managed remote file stores can be created quickly with just one command 
    d6tpipe.upsert_pipe(api, {'name': 'your-pipe'})

See :doc:`Pipes <../pipes>` for details. For creating self-hosted remotes, see :doc:`Advanced Pipes <../advremotes>`.

Share pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After you've registered a pipe, you can give others access to the remote data. By default only you have access so to share it with others you have to grant them access.

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()

    # give another user access
    settings = {"username":"another-user","role":"read"} # read, write, admin
    d6tpipe.upsert_permissions(api, 'your-pipe', settings)

    # make data repo public
    settings = {"username":"public","role":"read"}
    d6tpipe.upsert_permissions(api, 'your-pipe', settings)

See :doc:`Permissions <../permissions>` for details.

