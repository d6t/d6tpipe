Register and Administer Pipes
==============================================

Why register pipes?
---------------------------------------------

To have a pipe point to a remote data storage that you control, you have to register a pipe. Normally creating remote data storage is a complex task. But with d6tpipe it is exceptionally quick by using managed pipes which automatically take care of storage, authentication, permissioning etc, see :doc:`security <../security>` for details. To register self-hosted pipes, see :doc:`Advanced Pipes <../advremotes>`.

Register Managed Pipes
---------------------------------------------

To register a new pipe you need pipe settings that specify how remote data is stored and returned. The minimum requirement is the name which creates a free remote data repo. [todo: make d6tpipe default protocol]

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()

    response, data = d6tpipe.upsert_pipe(api, {'name': 'your-pipe'})


Customizing Pipe Settings
---------------------------------------------

Pipes not only store data but also control how data is stored, returned and provide additional meta data.

**Settings Parameters**

* ``name`` (str): unique name
* ``protocol`` (str): storage protocol (``d6tfree``,``s3``,``ftp``,``sftp``)
* ``options`` (json): 
    * ``dir`` (str): read/write from/to this subdirectory (auto created)
    * ``include`` (str): only include files with this pattern, eg ``*.csv`` or multiple with ``*.csv|*.xls``
    * ``exclude`` (str): exclude files with this pattern
* ``schema`` (json): see :doc:`Schema <../schema>`


Pipe Inheritance
---------------------------------------------

You can and should have multipe pipes that connect to the same underlying remote file storage but return different files. Why? Say you are a data vendor, you can create different pipes for different subscriptions without having to individually manage them. Say the vendors data files are:

| ``dataA\\monthly*.csv``  
| ``dataA\\daily*.csv``  
| ``dataB\\reports*.xlsx``  

Those are 3 different datasets, so you should define 3 separate pipes: ``vendor-monthly``, ``vendor-daily``, ``vendor-reports``. To avoid having to specify the same settings multiple times, you can create a parent pipe ``vendor-parent`` from which the child pipes can inherit settings.

.. code-block:: python

    settings_parent = {
        'name': 'vendor-parent',
        'protocol': 'd6tfree'
    }
    settings_monthly = {
        'name': 'vendor-monthly',
        'parent': 'vendor-parent',
        'options': {'dir':'dataA', 'include':'monthly*.csv'}
    }
    settings_daily = {
        'name': 'vendor-daily',
        'parent': 'vendor-parent',
        'options': {'dir':'dataA', 'include':'daily*.csv'}
    }
    settings_reports = {
        'name': 'vendor-reports',
        'parent': 'vendor-parent',
        'options': {'dir':'dataB', 'include':'reports*.xlsx'}
    }

This way the vendor can push files to ``vendor-parent`` and then grant clients individual access to ``vendor-monthly``, ``vendor-daily``, ``vendor-reports`` based on which product they have subscribed to.


Updating Pipe Settings
---------------------------------------------

Here is how you update an existing pipe with more advanced settings. This will either add the setting if it didn't exist before or overwrite the setting if it existed.

.. code-block:: python

    settings = \
    {
        'name': 'pipe-name',
        'remote': 'remote-name',
        'options': {
            'dir': 'some/folder',
            'include': '*.csv|*.xls',
            'exclude': 'backup*.csv|backup*.xls'
        },
        'schema': {
            'pandas': {
                'sep': ',',
                'encoding': 'utf8'
            }
        }
    }

    # update an existing pipe with new settings
    response, data = d6tpipe.upsert_pipe(api, settings)


Data Schemas
---------------------------------------------

When creating pipes you can add schema information see :doc:`Schema <../schema>`


Administer Pipes with repo API
---------------------------------------------

You can run any CRUD operations you can normally run on any REST API.

.. code-block:: python

    # listing pipes
    api.list_pipes() # names_only=False shows all details

    # CRUD
    response, data = api.cnxn.pipes.post(request_body=settings)
    response, data = api.cnxn.pipes._('pipe-name').get()
    response, data = api.cnxn.pipes._('pipe-name').put(request_body=new_settings)
    response, data = api.cnxn.pipes._('pipe-name').patch(request_body=new_settings)
    response, data = api.cnxn.pipes._('pipe-name').delete()

    # using pipe object
    response, data = pipe.cnxnpipe.get()
    response, data = pipe.cnxnpipe.put(request_body=all_settings)
    response, data = pipe.cnxnpipe.patch(request_body=mod_settings)
    response, data = pipe.cnxnpipe.delete()

