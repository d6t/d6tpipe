Read and Process Local Files
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
    df = pd.read_csv(pipe.dirpath/'test.csv') # open a file by name

    # open most recent file
    df = pd.read_csv(pipe.files(sortby='mod')[-1])

Applying File Filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can include file filters to find specific files.

.. code-block:: python

    files = pipe.files(include='data*.csv')
    files = pipe.files(exclude=['backup*.csv','oldstuff/*.csv'])

Read multiple files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can read multiple files at once.

.. code-block:: python

    # read multiple files into dask
    import dask.dataframe as dd
    files = pipe.filepaths(include='Advertising*.csv')
    ddf = dd.read_csv(files, **pipe.schema['dask'])
    print(ddf.head())


Process Files
---------------------------------------------

Write Processed Data to Pipe Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To keep all data in once place, it's best to save your processes data back to the pipe directory. That also makes it easier to :doc:`push files <../push>` later on if you choose to do so.

.. code-block:: python

    # save data to pipe
    df.to_csv(pipe.dirpath/'new.csv')
    df.to_csv(pipe.dirpath/'subdir/new.csv')


Advanced Topics
---------------------------------------------

Quickly accessing files without API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have pulled files but are unable to connect to the repo API, you can use `PipeLocal()` to access your files.

.. code-block:: python

    pipe = d6tpipe.PipeLocal('your-pipe')

Delete Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    pipe.delete_files_local() # delete files locally
    pipe.delete_files(files=['a.csv']) # delete a file locally and remotely
    pipe.reset() # reset local repo: delete all files and download
