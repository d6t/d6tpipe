Read and Process Local Files
==============================================

Show Local Files
---------------------------------------------

Files that are pulled from the remote get stored in a central local file directory. You can now easily access all your files from that central location.

.. code-block:: python

    pipe.dirpath # local file location
    pipe.filenames() # list all local files from remote
    pipe.scan_local() # list all files in local data repo

Read Local Files
---------------------------------------------



.. code-block:: python

    import pandas as pd
    df = pd.read_csv(pipe.dirpath/'test.csv') # open a file by name
    df = pd.read_csv(pipe.dirpath / pipe.files()[0])  # open a file by index

    import dask.dataframe as dd
    df = dd.read_csv(pipe.files()).compute() # read multiple files into dask

Process Files
---------------------------------------------

Using schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``schema`` holds parameters settings to quickly load your data. This is only available if it has been set up by the pipe owner.

.. code-block:: python

    # show schema
    print(pipe.schema)

    # use schema
    df = pd.read_csv(pipe.dirpath/'test.csv', **pipe.schema['pandas'])
    df = dd.read_csv(pipe.dirpath/'test.csv', **pipe.schema['dask'])
    df = pd.read_excel(pipe.dirpath/'others.xlsx', **pipe.schema['xls']['pandas'])


Write Processed Data to Pipe Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To keep all data in once place, it's best to save your processes data back to the pipe directory. That also makes it easier to :doc:`push files <../push>` later on if you choose to do so.

.. code-block:: python

    # save data to pipe
    df.to_csv(pipe.dirpath/'new.csv')
    df.to_csv(pipe.dirpath/'subdir/new.csv')


Advanced Topics
---------------------------------------------

Applying File Filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can include file filters to find specific files.

.. code-block:: python

    files = pipe.files(include='data*.csv')
    files = pipe.files(exclude=['backup*.csv','oldstuff/*.csv'])


Accessing without API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have pulled files but are unable to connect to the repo API, you can use `PipeLocal()` to access your files.

.. code-block:: python

    pipe = d6tpipe.PipeLocal('your-pipe')

Useful File Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below is a list of useful functions. See the reference :ref:`modindex` for details.

.. code-block:: python

    # other useful operations
    pipe.list_remote() # show files in remote
    pipe.list_remote(sortby='modified_at') # sorted by modified date
    pipe.delete_all_local() # reset local repo
    pipe.dbfiles.all() # inspect local files db
