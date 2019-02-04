# Databolt Pipeline

d6tpipe is a python library which makes it easier to exchange data files. It's like git for data! But better because you can include it in your data science code.

## Why use d6tpipe?

Sharing data files is common, for example between data vendors and consumers, data engineers and data scientists, teachers and students or desktop and laptop. 

But often the process is more cumbersome than you would like. With d6tpipe, in just a few lines of code, you can push and pull data files to/from a remote file store in a simple and unified framework. This allows you to separate data from code: code in git, data in data storage.

## What can d6tpipe do for you?

* Quickly create public and private remote file storage on AWS S3 and ftp
* Push/pull data to/from remote file storage to sync files and share with others
* Centrally manage data files across multiple projects
* Secure your data with permissions management and encrypting credentials
* Easily load and process files you have pulled

## Example Usage

Below is an example for pulling and loading machine learning data from an S3 bucket. It can be done in just a few commands directly from your python code.

For full quickstart instructions including setup, see [Quickstart documentation](https://d6tpipe.readthedocs.io/en/latest/quickstart.html)

```python

import d6tpipe
api = d6tpipe.api.APIClient() # see first-time setup to run

# find interesting datasets
api.list_pipes()
# ['intro-stat-learning']

# pull files from a data pipe with just one command
pipe = d6tpipe.Pipe(api, 'intro-stat-learning')
pipe.pull() 

'''
pulling: 0.52MB
100%|██████████| 10/10 [00:01<00:00,  8.25it/s]
['Advertising.csv',
 'Auto.csv',
 ...
 'README.md']
############### README ###############
# An Introduction to Statistical Learning
http://www-bcf.usc.edu/~gareth/ISL/data.html
############### README ###############
'''

# read a file into pandas from central repo with correct settings

import pandas as pd
df = pd.read_csv(pipe.dirpath/'Advertising.csv', **pipe.readparams['pandas']) 
df.head(2)      
'''
      TV  radio  newspaper  sales
1  230.1   37.8       69.2   22.1
2   44.5   39.3       45.1   10.4
'''

```

## Installation

Install with `pip install d6tpipe`. To update, run `pip install d6tpipe -U --no-deps`.

To install with ftp/sftp dependencies run `pip install d6tpipe[ftp]`.

You can also clone the repo and run `pip install .`

## First-time setup and registration

See https://d6tpipe.readthedocs.io/en/latest/quickstart.html#first-time-setup

## Documentation

http://d6tpipe.readthedocs.io

## Data Security

See https://d6tpipe.readthedocs.io/en/latest/security.html

## d6tflow Integration

Getting files is often just the first part of a data science workflow. To help you with subsequent processing of data, we recommend you make use of [d6tflow](https://github.com/d6t/d6tflow). See [Sharing Workflows and Outputs](https://d6tflow.readthedocs.io/en/latest/collaborate.html).

## Faster Data Engineering

Check out other d6t libraries to solve common data engineering problems, including  
* data ingest: quickly ingest raw data
* fuzzy joins: quickly join data
* data workflows: quickly build complex data workflows

https://github.com/d6t/d6t-python

## Get notified

`d6tpipe` is in active development. Join the [databolt blog](http://blog.databolt.tech) for the latest announcements and tips+tricks.

## License and Terms

The client library is under MIT license. If you use managed resources, in particular managed remote file storage, see [Terms](https://www.databolt.tech/index-terms.html) and [Privacy](https://www.databolt.tech/index-terms.html#privacy).

## Collecting Errors Messages and Usage statistics

We have put a lot of effort into making this library useful to you. To help us make this library even better, it collects ANONYMOUS error messages and usage statistics. See [d6tcollect](https://github.com/d6t/d6tcollect) for details including how to disable collection. Collection is asynchronous and doesn't impact your code in any way.

It may not catch all errors so if you run into any problems or have any questions, please raise an issue on github.