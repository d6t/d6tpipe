Add and Push Files
==============================================

Adding Data to a Pipe
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

After you've added data to a pipe (see above), pushing data is just as simple as pulling data. You do need write permissions to the pipe for it to work though.

.. code-block:: python

    pipe.push_preview() # preview
    pipe.push() # execute

Expired Token
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you get this error: ``ClientError: An error occurred (ExpiredToken) when calling the PutObject operation: The provided token has expired.``. Just reconnect to the pipe. For security reasons you receive short-term credentials which are renewed when you reconnect. You do still need to have write access else reconnecting might fail.

Creating pipes and remotes
---------------------------------------------

If you want to create your own pipes and remotes, see :doc:`Remotes <../remotes>` and :doc:`Pipes <../pipes>`.

Other Useful Pipe Operations
---------------------------------------------

Below is a list of useful functions. See the reference :ref:`modindex` for details.

.. code-block:: python

    # advanced push options
    pipe.push(['a.csv']) # force push/pull on selected files
    pipe.push(include='*.csv',exclude='backup*.csv') # apply file filters

Removing files
---------------------------------------------

The best way to remove files is to manually remove them locally or remotely and then remove orphans. **Make sure BEFORE you delete any files, you do a pull.**

.. code-block:: python

    # remove orphan files
    pipe.remove_orphans('local') # remove local orphans
    pipe.remove_orphans('remote') # remove remote orphans
    pipe.remove_orphans('both') # remove all orphans

You can also force remove remote files. DANGER! You will probably mess things up and probably need to reset the pipe.

.. code-block:: python

    pipe._pullpush_luigi(['filename'],op='remove')

If you've messed things up, run this:

.. code-block:: python

    pipe.delete_all_local() # reset local repo
    pipe = d6tpipe.pipe.Pipe(api, 'pipe-name', mode='all')
    pipe.pull() # pull all files
    pipe.setmode('default')

See the reference :ref:`modindex` for details.
