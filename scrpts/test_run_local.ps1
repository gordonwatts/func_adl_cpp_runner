# Simple set of commands that will run the docker containers all the way through.
# Note, these are expected to be local and meaningul. This is NOT a production
# script. It will almost certianly not work on your system!
#

# Config locations
$scratch = "G:\testing"
$inputfile = 

# Make sure the proper directories exist in the scratch area
New-Item -ItemType Directory -Force -Path $scratch\ast
New-Item -ItemType Directory -Force -Path $scratch\cpp_cache
New-Item -ItemType Directory -Force -Path $scratch\results

# Next, lets make an ast!
docker run -t --rm -v $scratch/ast:/ast  gordonwatts/func_adl:v0.0.1 /bin/bash -c "python write_ast.py 0 /ast/ast0.pickle"

# Next, lets turn it into C++. This guy returns some stuff to stdoutput.
# NOTE: first time you run this it will produce warnings, and those accidentally get picked up by cpp_info. The second time
# it reuses a cache, and will correctly get the hash. So run twice. Yeah, I know. But this is just a test!
$cpp_info = $(docker run -t --rm -v $scratch/ast:/ast -v $scratch/cpp_cache:/cache gordonwatts/func_adl:v0.0.1 python translate_ast_to_cpp.py /ast/ast0.pickle /cache)
$hash = $cpp_info[0]
$runner = $cpp_info[1]

# Now, run the code
docker run -it --rm `
    -v $scratch/cpp_cache:/cache `
    -v G:\GRIDDocker:/data `
    -v $scratch/results:/results `
    gordonwatts/func_adl_cpp_runner:v0.0.1 `
    /bin/bash -c "source /home/atlas/release_setup.sh; pwd; cd /home/atlas; ls; touch filelist.txt; python /usr/src/app/cmd_runner.py /cache/$hash /cache/$hash/$runner /data/mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r9364_r9315_p3795/DAOD_EXOT15.17545434._000001.pool.root.1"

Write-Host "Find results in $scratch/results!"