Create and Manage Remotes
==============================================

What is a Remote?
---------------------------------------------

A remote refers to a remote data store, like s3 or ftp. To pipe any data, you first have to define the remote source from which the data will be piped from. Once you have a remote, you can define one or more data pipes that share the same remote.

Creating a Managed Remote
---------------------------------------------

d6tpipe offers managed data repos on S3. They are easy to get set up, don't require an AWS account and you can easily manage permissions. If instead you prefer to self-host see below. But managed remotes are a good way to get started.

.. code-block:: python

    # managed data repo can be quickly created with just one command 
    d6tpipe.api.create_pipe_with_remote(api, {'name': 'your-data-files', 'protocol': 'd6tfree'})


Managing Remote Access Permissions
---------------------------------------------

Initially only you will be able to access the data. To share the data with others you will have to grant permissions.

.. code-block:: python

    # give another user access
    settings = {"username":"another-user","role":"read"} # read, write, admin
    d6tpipe.create_or_update_permissions(api, 'remote-name', settings)

    # make data repo public
    settings = {"username":"public","role":"read"}
    d6tpipe.create_or_update_permissions(api, 'remote-name', settings)

    # view permissions (owner only)
    pipe.cnxnremote.permissions.get()
    api.cnxn.remotes._('remote-name').permissions.get()
    

**Parameters**

* ``user`` (str): d6tpipe username  
    * ``public`` (str): special name to manage public permissions  
* ``role`` (str): 
    * ``read`` (str): can read data from repo but not write
    * ``write`` (str): can write data as well as read
    * ``admin`` (str): can change settings in addition to read and write


Updating Remote Settings
---------------------------------------------

Here is how you update an existing remote with more advanced settings. This will either add the setting if it didn't exist before or overwrite the setting if it existed.

.. code-block:: python

    settings = \
    {
        'readParams': {
            'pandas': {
                'sep': ',',
                'encoding': 'utf8'
            }
        }
    }

    # update an existing pipe with new settings
    response, data = d6tpipe.api.create_or_update(api.cnxn.remotes, settings)


Managing Remotes
---------------------------------------------

You can run any CRUD operations you can normally run on any REST API.

.. code-block:: python

    # listing remotes
    api.list_remotes() # names_only=False shows all details

    # CRUD
    response, data = api.cnxn.remotes.post(request_body=settings)
    response, data = api.cnxn.remotes._('remote-name').get()
    response, data = api.cnxn.remotes._('remote-name').put(request_body=new_settings)
    response, data = api.cnxn.remotes._('remote-name').patch(request_body=new_settings)
    response, data = api.cnxn.remotes._('remote-name').delete()
    # instead of api.cnxn.remotes, you can also use pipe.cnxnremote

Show All Remote Files
---------------------------------------------

Normally you want to list remote files via a pipe. But you might need to explore all files in order to configure your pipe. Here is a quick recipe for showing all files on a remote. The details of pipes are covered in the next section.

.. code-block:: python

    settings = {
        'name': 'show-files',
        'remote': 'remote-name',
    }

    d6tpipe.api.create_or_update(api.cnxn.remotes, settings)
    d6tpipe.Pipe(api, 'show-files').scan_remote() # show all files


Using Self-hosted Remotes
---------------------------------------------

See :doc:`Advanced Remote Operations <../advremotes>`
