# s3_perf
S3 benchmark.

## Dependencies
* aws-sdk-cpp
* gflags

## Running
* Create `~/.aws/credentials` file as follows:
```sh
[default]
aws_access_key_id = <access_key>
aws_secret_access_key = <secret_key>
```

* Depending on the environment, you may need to specify the library path:
```sh
LD_LIBRARY_PATH=/usr/local/lib64 ./s3_perf
```
