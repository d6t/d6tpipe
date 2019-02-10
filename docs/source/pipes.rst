Create and Manage Pipes
==============================================

What is a pipe, technically?
---------------------------------------------

Conceptually, a pipe is a "visualization" layer on top of a  :doc:`remote <../remotes>`. You can and should have multipe pipes that connect to the same remote. In other words, a pipe is conceptually similar to a dataset, that is files that **logically belong together** and have similar read parameters.

Say for example your remote has the file structure:

| ``dataA\\daily*.csv``  
| ``dataA\\monthly*.csv``  
| ``dataB\\reports*.xlsx``  

Those are 3 different datasets, so you should define 3 separate pipes: ``dataA-daily``, ``dataA-monthly``, ``dataB-reports``.


Creating Pipes
---------------------------------------------

**Before you can create a pipe, you need to** :doc:`create a remote <../remotes>` or have admin permissions to one.

Creating pipes is very similar to creating remotes, you POST to the rest API. 

.. code-block:: python

    import d6tpipe.api
    import d6tpipe.pipe

    api = d6tpipe.api.APIClient()

    settings = \
    {
        'name': 'pipe-name',
        'remote': 'remote-name',
    }

    response, data = d6tpipe.api.create_or_update(api.cnxn.pipes, settings)


**Parameters**

* ``name`` (str): unique name
* ``remote`` (str): name of remote
* ``settings`` (json): 
    * ``pipedir`` (str): read/write from/to this subdir (auto created)
    * ``include`` (str): only include files with this pattern, eg ``*.csv`` or multiple with ``*.csv|*.xls``
    * ``exclude`` (str): exclude files with this pattern
* ``readParams`` (json): any parameters you want to pass to the reader eg pandas


readParams
---------------------------------------------

``readparams`` settings takes any data so you can use that in a variety of ways to pass parameters to the reader. This makes it easier for the data recipient to process your data files.

.. code-block:: python

    settings = \
    {
        'readParams': {
            'pandas': {
                'sep': ',',
                'encoding': 'utf8'
            },
            'dask': {
                'sep': ',',
                'encoding': 'utf8'
            },
            'xls': {
                'pandas': {
                    'sheet_name':'Sheet1'
                }
            }
        }
    }

    pipe.cnxnpipe.patch(settings) # update settings

The flexibility is good but you might want to consider adhering to metadata specifications such as https://frictionlessdata.io/specs/.


Updating Pipe Settings
---------------------------------------------

Here is how you update an existing pipe with more advanced settings. This will either add the setting if it didn't exist before or overwrite the setting if it existed.

.. code-block:: python

    settings = \
    {
        'name': 'pipe-name',
        'remote': 'remote-name',
        'settings': {
            'remotedir': 'some/folder',
            'include': '*.csv|*.xls',
            'exclude': 'backup*.csv|backup*.xls'
        },
        'readParams': {
            'pandas': {
                'sep': ',',
                'encoding': 'utf8'
            }
        }
    }

    # update an existing pipe with new settings
    response, data = d6tpipe.api.create_or_update(api.cnxn.pipes, settings)


Managing Pipes with repo API
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

