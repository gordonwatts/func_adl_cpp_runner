# This script will run everything in plain docker.
# This is for debugging and testing. Kubernetes is probably the right place for this.
# And at some point there will be a helm chart that will properly run all this.

# How to insert an ast into the queue by hand.
#  == "0" is the ast number (only 0 is supported so far)
#  == "172.17.0.2" is the ip address of your docker container that is running rabbit. Use
#     'docker inspect' as-rabbit to see what it is.
# docker run --rm -it gordonwatts/func_adl:v0.0.1 python send_ast.py 0 172.17.0.2 find_did

# Start up rabbit for all the interprocess communication.
# Using default username/password (guest/guest) by going to http://localhost:15672.
docker run --rm -d --hostname as-rabbit --name as-rabbit -p 15672:15672 rabbitmq:3-management

# First go at getting the dataset downloader up and running.
# rabbit find_did -> parse_cpp
docker run --rm -it -v H:\OneDrive\.ssh\rucio-config\usercert:/root/rawcert -v G:\GRIDDocker:/data --name=desktop-rucio --rm -it --entrypoint /bin/bash  gordonwatts/desktop-rucio:alpha-0.1.3 startup_rabbit.sh gwatts mass2000 atlas file:///data 172.17.0.2

# Next, build the C++ code that is going to do our work
# rabbit parse_cpp -> run_cpp
docker run -it --rm -v G:\testing\cpp_cache:/cache  gordonwatts/func_adl:v0.0.1 python translate_ast_to_cpp_rabbit.py 172.17.0.2

# Finally, the runner that will deposit the analysis in an output directory (I know, they will all overwrite, but this is good for now).
docker run -it --rm  -v G:\testing\cpp_cache:/cache -v G:\GRIDDocker:/data -v G:\testing\results:/results  gordonwatts/func_adl_cpp_runner:v0.0.1 /bin/bash -c "source /home/atlas/release_setup.sh; python cmd_runner_rabbit.py 172.17.0.2"
