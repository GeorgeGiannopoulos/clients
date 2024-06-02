# Storage Bucket Client

A client that connects to [Bucket Service](https://github.com/GeorgeGiannopoulos/bucket)

## How to install

For Production:

```shell
pip install -r requirements.txt
pip install .
```

For Deployment:

```shell
pip install -r requirements.txt
pip install -e .
```

## How to use

```python
from bucket_client.client import BucketClient
bucket = BucketClient(host='localhost', port=8000)

# Upload a file
response = bucket.upload_file("/path/to/trained_model.pikl")

# Update a file
response = bucket.update_file("/path/to/trained_model.pikl")

# Get a file (download)
response = bucket.get_file("trained_model.pikl")
binary = response.file()
filepath = response.save('/path/to/directory/where/the/file/will/be/stored')  # If path is None then './'

# Delete a file
response = bucket.delete_file("trained_model.pikl")
```

## How to test

**TODO**
