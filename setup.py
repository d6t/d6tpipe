from setuptools import setup

setup(
    name='d6tpipe',
    version='1.1.2',
    packages=['d6tpipe','d6tpipe.http_client','d6tpipe.luigi','d6tpipe.utils'],
    url='https://github.com/d6t/d6tpipe',
    license='MIT',
    author='DataBolt Team',
    author_email='support@databolt.tech',
    description='d6tpipe is a python library which makes it easier to exchange data',
    long_description='d6tpipe is a python library which makes it easier to exchange data. For example between data vendors and consumers, data engineers and data scientists, teachers and students or desktop and laptop. In just a few lines of code, you can push and pull data to/from S3 and ftp in a simple and unified framework.',
    install_requires=[
        'luigi','tinydb','boto3','botocore','tinydb-serialization','cachetools','pyjwt','pyyaml','tqdm','d6tcollect'
    ],
    extras_require={
    'ftp': ['pysftp','pyftpsync']},
    include_package_data=True,
    python_requires='>=3.5',
    keywords=['d6tpipe', 'data-pipes'],
    classifiers=[]
)
