# func_adl_cpp_runner

A docker container that will pick a C++ adl job off a queue and run it in the ATLAS environment.

Building
========

Use the following command to build the docker image

```
docker build --rm -f "Docker\Dockerfile" -t gordonwatts/func_adl_cpp_runner:v0.0.1 .
```
