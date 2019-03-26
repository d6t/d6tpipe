Sharing Remote Data Storage
==============================================

Managing Remote Access Permissions
---------------------------------------------

Initially only you will be able to access the data. To share the data with others you will have to grant permissions. If you give access to a parent pipe, users will have access to all child pipes.

.. code-block:: python

    # give another user access
    settings = {"username":"another-user","role":"read"} # read, write, admin
    d6tpipe.upsert_permissions(api, 'pipe-name', settings)

    # make data repo public
    settings = {"username":"public","role":"read"}
    d6tpipe.upsert_permissions(api, 'remote-name', settings)

    # view permissions (owner only)
    pipe.cnxnpipe.permissions.get()
    

Access Roles
---------------------------------------------

You can specify what level of access a user has to your pipe.

* ``role`` (str): 
    * ``read`` (str): can read data from repo but not write
    * ``write`` (str): can write data as well as read
    * ``admin`` (str): can change settings in addition to read and write

Invite User with email
---------------------------------------------

If users are not registered with d6tpipe or you only know their email, you can grant access via email.

.. code-block:: python

    # give another user access
    settings = {"email":"name@client.com","role":"read"} # read, write, admin
    d6tpipe.upsert_permissions(api, 'pipe-name', settings)

