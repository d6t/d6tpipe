Read and Write Local Files
==============================================

Show Local Files
---------------------------------------------

Files that are pulled from the remote get stored in a central local file directory. 

.. code-block:: python

    pipe.dirpath # where local files are stored
    pipe.files() # show synced local files
    pipe.scan_local() # show all files in local storage

Read Local Files
---------------------------------------------

You now have a lot of powerful functions to easily access all your files from a central location across multiple projects.

.. code-block:: python

    import pandas as pd

    # open a file by name
    df = pd.read_csv(pipe.dirpath/'test.csv')

    # open most recent file
    df = pd.read_csv(pipe.files(sortby='mod')[-1])

Applying File Filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can include file filters to find specific files.

.. code-block:: python

    files = pipe.files(include='data*.csv')
    files = pipe.files(exclude='*.xls|*.xlsx')

Read multiple files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can read multiple files at once.

.. code-block:: python

    # read multiple files into dask
    import dask.dataframe as dd
    files = pipe.filepaths(include='Advertising*.csv')
    ddf = dd.read_csv(files, **pipe.schema['dask'])
    print(ddf.head())


Quickly accessing local files without connecting to API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have pulled files and don't always want to or can't connect to the repo API, you can use `PipeLocal()` to access your local files.

.. code-block:: python

    pipe = d6tpipe.PipeLocal('your-pipe')

Write Local Files
---------------------------------------------

Write Processed Data to Pipe Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To keep all data in once place, it's best to save your processes data back to the pipe directory. That also makes it easier to :doc:`push files <../push>` later on if you choose to do so.

.. code-block:: python

    # save data to pipe
    df.to_csv(pipe.dirpath/'new.csv')
    df.to_csv(pipe.dirpath/'subdir/new.csv')


Delete Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    pipe.delete_files_local() # delete all local files
    pipe.delete_files_remote() # delete all remote files
    pipe.delete_files(files=['a.csv']) # delete a file locally and remotely
    pipe.reset() # reset local repo: delete all files and download

    # remove orphan files
    pipe.remove_orphans('local') # remove local orphans
    pipe.remove_orphans('remote') # remove remote orphans
    pipe.remove_orphans('both') # remove all orphans

Reset Local Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    pipe.reset() # will force pull all files
    pipe.reset(delete=True) # delete all local files before pull
