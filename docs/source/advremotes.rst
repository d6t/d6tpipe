Advanced: Self-hosted Remotes
==============================================

Creating Self-hosted Remotes
---------------------------------------------

You can have users push/pull from your own S3 and (s)ftp resources. The repo API stores all the neccessary details so the data consumer does not have to make any changes if you make any changes to the remote storage.

.. code-block:: python

    import d6tpipe.api
    import d6tpipe.pipe

    api = d6tpipe.api.APIClient()

    settings = \
    {
        'name': 'remote-name',
        'protocol': 's3',
        'location': 'bucket-name',
        'credentials' : {
            'aws_access_key_id': 'AAA', 
            'aws_secret_access_key': 'BBB'
        }
    }

    d6tpipe.api.create_or_update(api.cnxn.remotes, settings)

Most resources in d6tpipe are managed in an REST API type interface so you will be using the API client to define resources like remotes, pipes, permission etc. That way you can easily switch between local and server deployment without having to change your code.

Parameters
^^^^^^^^^^^^^^^^

* ``name`` (str): unique id
* ``protocol`` (str): [s3, ftp, sftp]
* ``location`` (str): s3 bucket, ftp server name/ip
* ``credentials`` (json): credentials for pulling. s3: aws_access_key_id, aws_secret_access_key. ftp: username, password
* ``settings`` (json): any settings to be shared across pipes
    * ``dir`` (str): read/write from/to this subdir (auto created)
* ``schema`` (json): any parameters you want to pass to the reader


Access Control
---------------------------------------------

You can have separate read and write credentials

.. code-block:: python

    settings = \
    {
        'name': 'remote-name',
        'protocol': 's3',
        'location': 'bucket-name',
        'credentials': {
            'read' : {
                'aws_access_key_id': 'AAA', 
                'aws_secret_access_key': 'BBB'
            },
            'write' : {
                'aws_access_key_id': 'AAA', 
                'aws_secret_access_key': 'BBB'
            }
        }
    }


Keeping Credentials Safe
---------------------------------------------

Don't Commit Credentials To Source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In practice you wouldn't want to have the credentials in the source code like in the example above. It's better to load the settings from a json, yaml or ini file to a python dictionary that you can pass to the REST API. Alternatively for server-based setups you can work with REST tools like Postman.

Here is a recipe for loading settings from json and yaml files.

.. code-block:: python

    # create file
    (api.repopath/'.creds.json').touch()

    # edit file in `api.repo` folder. NB: you don't have to use double quotes in the json but you have to use spaces for tabs
    print(api.repo)

    # load settings and create
    settings = d6tpipe.utils.loadjson(api.repopath/'.creds.json')['pipe-name']
    d6tpipe.api.upsert_pipe(api, settings)

    # or if you prefer yaml
    (api.repopath/'.creds.yaml').touch()
    settings_remote = d6tpipe.utils.loadyaml(api.repopath/'.creds.json')['pipe-name']
    d6tpipe.api.upsert_pipe(api, settings)

See example templates in https://github.com/d6t/d6tpipe/tree/master/docs


Premium Features
---------------------------------------------

See :doc:`Premium Features <../premium>` to gain access to premium features.

Encrypting Credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**By default, credentials are stored in clear text.**

To keep your credentials safe, especially on the cloud API, you can encrypt them which is very easy easy to do with :meth:`api.encode`.

.. code-block:: python

    d6tpipe.api.create_or_update(api.cnxn.remotes, api.encode(settings))

This uses an encryption key which is auto generated for you, you can update that key if you like, see config section. If you change the encryption key, you will have to recreate all encrypted pipes and remotes

Any form of security has downsides, here is how encrypting credentials impacts functionality:  

* If you lose the encryption key, you will have to recreate all encrypted pipes and remotes  
* All operations have to take place locally, that is you can't schedule any sync or mirroring tasks on the sever because it won't be able to access the source  
* You won't be able to share any credentials with other users unless they have your encryption key.  


Managing Your Encryption Key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A key is used to encrypt the data. A random key is generated automatically but you can change it, eg if you want to share it across your team.

.. code-block:: python
    
    # get key
    api.key

    # set key
    d6tpipe.api.ConfigManager().update({'key':'yourkey'})


Creating d6tpipe Compatible S3 buckets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

d6tpipe comes batteries included with convenience functions to set up s3 buckets with appropriate users and permissions. It creates a read and write user with API credentials that can be directly passed into the REST API.

.. code-block:: python

    session = boto3.session.Session(
        aws_access_key_id=settings['AAA'],
        aws_secret_access_key=settings['BBB'],
    )
    settings = d6tpipe.utils.s3.create_bucket_with_users(session, 'remote-name')
    d6tpipe.api.create_or_update(api.cnxn.remotes, settings)

See module refernce for details including how to customize. In case you have trust issues, you can inspect the source code to see what it does.

The AWS session need to refer to a user with the following permissions. If you customize ``d6tpipe.utils.s3`` parameters you might have to amend this. The lazy way of doing is this to create the AWS session with your AWS root keys.

.. code-block:: python

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "iam",
                "Effect": "Allow",
                "Action": [
                    "iam:DeleteAccessKey",
                    "iam:DeleteUser",
                    "iam:GetUser",
                    "iam:CreateUser",
                    "iam:CreateAccessKey",
                    "iam:ListAccessKeys"
                ],
                "Resource": "d6tpipe-*"
            },
            {
                "Sid": "VisualEditor0",
                "Effect": "s3-detail",
                "Action": [
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:HeadBucket"
                ],
                "Resource": "arn:aws:s3:::d6tpipe-*/*"
            },
            {
                "Sid": "s3-bucket",
                "Effect": "Allow",
                "Action": [
                    "s3:CreateBucket",
                    "s3:GetBucketPolicy",
                    "s3:PutBucketPolicy",
                    "s3:ListBucket",
                    "s3:DeleteBucket"
                ],
                "Resource": "arn:aws:s3:::d6tpipe-*"
            }
        ]
    }

Removing d6tpipe S3 buckets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # to remove bucket and user
    d6tpipe.utils.s3.delete_bucket(session, 'd6tpipe-[remote-name]')
    d6tpipe.utils.s3.delete_user(session, 'd6tpipe-[remote-name]-read')
    d6tpipe.utils.s3.delete_user(session, 'd6tpipe-[remote-name]-write')

