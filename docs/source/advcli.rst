Advanced: Non-python Access
==============================================

Access REST API
------------------------------

The :doc:`repo API <../connect>` is a standard REST API that you can access with any tool that you normally use to interface with APIs like postman or curl. 

To authenticate pass ``Authorization Token {token value}``. You can get your API token http://pipe.databolt.tech/gui/api-access.

Example with curl using the demo account

.. code-block:: bash

    curl -H "Authorization: Token 69337d229e1e35677623341973a927518a6abc7c" https://pipe.databolt.tech/v1/api/pipes/ 

REST API Documentation
------------------------------

Swagger documentation is provided at https://pipe.databolt.tech/swagger/

AWS CLI
------------------------------

d6tpipe uses short-term credentials to secure data pipes. That means you have to request new credentials from the REST API. To use the AWS CLI, add the latest credentials to environmental variables and then you can use the AWS CLI as usual. For more details see https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html#using-temp-creds-sdk-cli

.. code-block:: bash

    c = pipe._get_credentials() # pass write=True for write credentials
    # or GET https://pipe.databolt.tech/v1/api/pipes/{pipe_name}/credentials/?role=read

    # linux - set env vars
    print(f'export AWS_ACCESS_KEY_ID={c["aws_access_key_id"]}')
    print(f'export AWS_SECRET_ACCESS_KEY={c["aws_secret_access_key"]}')
    print(f'export AWS_SESSION_TOKEN={c["aws_session_token"]}')

    # windows - set env vars
    print(f'SET AWS_ACCESS_KEY_ID={c["aws_access_key_id"]}')
    print(f'SET AWS_SECRET_ACCESS_KEY={c["aws_secret_access_key"]}')
    print(f'SET AWS_SESSION_TOKEN={c["aws_session_token"]}')

    # use aws cli
    print(f"aws s3 ls {pipe.settings['options']['remotepath']}")
