Manage Data Schemas
==============================================

Create Schema
---------------------------------------------

``schema`` settings takes any data so you can use that in a variety of ways to pass parameters to the reader. This makes it easier for the data recipient to process your data files.

.. code-block:: python

    settings = \
    {
        'schema': {
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

    pipe.update_settings(settings) # update settings

The flexibility is good but you might want to consider adhering to metadata specifications such as https://frictionlessdata.io/specs/.

Using schema
---------------------------------------------

``schema`` holds parameters settings to quickly load your data. This is only available if it has been set up by the pipe owner.

.. code-block:: python

    # show schema
    print(pipe.schema)

    # use schema
    df = pd.read_csv(pipe.dirpath/'test.csv', **pipe.schema['pandas'])
    df = dd.read_csv(pipe.dirpath/'test.csv', **pipe.schema['dask'])
    df = pd.read_excel(pipe.dirpath/'others.xlsx', **pipe.schema['xls']['pandas'])


