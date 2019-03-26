Push Files from Local to Remote
==============================================

Adding Local Files
---------------------------------------------

To add data to a pipe, you need to add files to the central pipe directory at ``pipe.dir``. d6tpipe makes it easy to add new data with a set of convenience functions. 

.. code-block:: python

    pipe = d6tpipe.pipe.Pipe(api, 'test')

    # save new data directly to pipe
    df.to_csv(pipe.dirpath/'new.csv')

    # import files from another folder
    pipe.import_dir('/some/folder'))

    # import files a list of files
    pipe.import_files(glob.iglob('folder/**/*.csv'), move=True)
    

You can also use any other (manual) methods to copy files to the pipe directory ``pipe.dir``.


Pushing Data
---------------------------------------------

After you've added data to a pipe (see above), pushing data is just as simple as pulling data. 

.. code-block:: python

    pipe.push_preview() # preview
    pipe.push() # execute

For this to work, you do need to have :doc:`write permissions <../permissions>` to the pipe or :doc:`registered your own pipe <../pipes>`.

Expired Token
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you might get this error: ``ClientError: An error occurred (ExpiredToken) when calling the PutObject operation: The provided token has expired.``. For security reasons you receive short-term credentials. You can force renewal 

.. code-block:: python

    pipe._reset_credentials()


Customize Push
---------------------------------------------

You can manually control what gets pushed.

.. code-block:: python

    # advanced push options
    pipe.push(['a.csv']) # force push/pull on selected files
    pipe.push(include='*.csv',exclude='backup*.csv') # apply file filters

