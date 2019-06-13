# Run an executable from the docker command line.
import sys
import os

def run_from_cmdline(source_files, main_script, input_files):
    'Run the input files on the job given.'
    # Queue up the output files
    with open('filelist.txt', 'w') as f:
        for f_name in input_files:
            f.write(f_name + '\n')
    
    os.system(main_script)

if __name__ == "__main__":
    bad_args = len(sys.argv) < 4
    bad_args = bad_args or not os.path.isdir(sys.argv[1])
    bad_args = bad_args or any(not os.path.isfile(f) for f in sys.argv[2:])

    if bad_args:
        print "Usage: python cmd_runner.py <cpp_source_files> <main-script-file> <input-file-1> <input-file-2> ..."
        print "       The ccp source files is a directory that contains the main script file and also other soruce files as written by the funciontal_adl docker image"
        print "       Each input file is the path to a physical file or URL (xrootd) accessible from inside the container"
    else:
        run_from_cmdline (sys.argv[1], sys.argv[2], sys.argv[3:])