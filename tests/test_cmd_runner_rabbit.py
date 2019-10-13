# Some tests to run against the guy.
import os
import sys
import json
import tempfile
import zipfile
import base64
from pytest import fixture
import shutil
from mock import MagicMock
sys.path += ['tools']
from cmd_runner_rabbit import process_message  # noqa: E402


def zipdir(path, zip_handle):
    # zip_handle is zipfile handle
    for root, _, files in os.walk(path):
        for file in files:
            zip_handle.write(os.path.join(root, file), file)


@fixture()
def request_body():
    'A standard request body'

    # Build up a zip file with data to run against
    z_filename = 'good_request_body.zip'
    d = tempfile.mkdtemp()
    try:
        with open(os.path.join(d, 'f1.txt'), 'w') as f_out:
            f_out.write('hi there\n')

        zip_h = zipfile.ZipFile(z_filename, 'w', zipfile.ZIP_DEFLATED)
        zipdir(d, zip_h)
        zip_h.close()
    finally:
        shutil.rmtree(d)

    with open(z_filename, 'rb') as b_in:
        zip_data = b_in.read()
        zip_data_b64 = bytes.decode(base64.b64encode(zip_data))

    # Create the result json to send back
    result = {
        'hash': '1234568719',
        'hash_source': '1234',
        'main_script': 'runit.sh',
        'files': ['file://file1.root'],
        'output_file': 'analysis01.root',
        'file_data': zip_data_b64,
        'treename': 'bogus'
    }
    return json.dumps(result)


@fixture()
def rabbit_info():
    'Return the rabbit communication channels'
    connection = MagicMock()
    connection.add_callback_threadsafe = lambda a: a()
    channel = MagicMock()
    method = MagicMock()
    return (channel, connection, method)


@fixture()
def returns_successful(monkeypatch):
    'Returns a good run with status zero'
    monkeypatch.setattr('os.system', lambda cmd: 0)
    yield None


def write_logfile(text, cmd, cmd_return):
    'Helper func to write out the log file to the correct place'
    with open(cmd[cmd.find("tee ") + 4:], 'w') as output:
        output.write(text)
    return cmd_return


cpp_build_log = '\n'.join(['Configured GCC from: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6', 'Configured AnalysisBase from: /usr/AnalysisBase/21.2.62/InstallArea/x86_64-slc6-gcc62-opt', '-- The C compiler identification is GNU 6.2.0', '-- The CXX compiler identification is GNU 6.2.0', '-- Check for working C compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/gcc', '-- Check for working C compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/gcc -- works', '-- Detecting C compiler ABI info', '-- Detecting C compiler ABI info - done', '-- Detecting C compile features', '-- Detecting C compile features - done', '-- Check for working CXX compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/g++', '-- Check for working CXX compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/g++ -- works', '-- Detecting CXX compiler ABI info', '-- Detecting CXX compiler ABI info - done', '-- Detecting CXX compile features', '-- Detecting CXX compile features - done', '-- Found AnalysisBase: /usr/AnalysisBase/21.2.62/InstallArea/x86_64-slc6-gcc62-opt (version: 21.2.62)', '-- Found AnalysisBaseExternals: /usr/AnalysisBaseExternals/21.2.62/InstallArea/x86_64-slc6-gcc62-opt (version: 21.2.62)', '-- Setting ATLAS specific build flags', '-- checker_gccplugins library not found', '-- Package(s) in AnalysisBaseExternals: 19', '-- Using the LCG modules without setting up a release', '-- Package(s) in AnalysisBase: 173', '-- Looking for pthread.h', '-- Looking for pthread.h - found', '-- Looking for pthread_create', '-- Looking for pthread_create - not found', '-- Check if compiler accepts -pthread', '-- Check if compiler accepts -pthread - yes', '-- Found Threads: TRUE  ', '-- Configuring ATLAS project with name "UserAnalysis" and version "1.0.0"', '-- Using build type: RelWithDebInfo', '-- Using platform name: x86_64-slc6-gcc62-opt', '-- Unit tests will be built by default', '-- Found 1 package(s)', '-- Using the LCG modules without setting up a release', '-- Considering package 1 / 1', '-- No package filtering rules read', '-- Configuring the build of package: analysis', '-- Number of packages configured: 1', '-- Time for package configuration: 0 seconds', '-- Using the LCG modules without setting up a release', '-- Including the packages from project AnalysisBase - 21.2.62...', '-- Including the packages from project AnalysisBaseExternals - 21.2.62...', '-- Using the LCG modules without setting up a release', '-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/packages.txt', '-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/compilers.txt', '-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/ReleaseData', '-- Generating external environment configuration', '-- Using the LCG modules without setting up a release', '-- Writing runtime environment to file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/env_setup.sh', '-- Configuring done', '-- Generating done', '-- Build files have been written to: /home/atlas/rel/build', 'Scanning dependencies of target queryDictDictGen', '[ 11%] Generating queryDictReflexDict.cxx', '[ 11%] Built target queryDictDictGen', 'Scanning dependencies of target UserAnalysisRootMapMerge', '[ 22%] Built UserAnalysisRootMapMerge', '[ 22%] Built target UserAnalysisRootMapMerge', 'Scanning dependencies of target Package_analysis_tests', '[ 22%] Built target Package_analysis_tests', 'Scanning dependencies of target atlas_tests', '[ 22%] Built target atlas_tests', 'Scanning dependencies of target analysisScriptsInstall', '[ 33%] Generating ../x86_64-slc6-gcc62-opt/bin/ATestRun_eljob.py', '[ 33%] Built target analysisScriptsInstall', 'Scanning dependencies of target analysisLib', '[ 44%] Building CXX object analysis/CMakeFiles/analysisLib.dir/Root/query.cxx.o', "/home/atlas/rel/source/analysis/Root/query.cxx: In member function 'virtual StatusCode query::execute()':", "/home/atlas/rel/source/analysis/Root/query.cxx:69:26: error: 'const class xAOD::Jet_v1' has no member named 'ptt'; did you mean 'pt'?", '       _JetPt4 = (i_obj3->ptt()/1000.0);', '                          ^~~', 'make[2]: *** [analysis/CMakeFiles/analysisLib.dir/Root/query.cxx.o] Error 1', 'make[1]: *** [analysis/CMakeFiles/analysisLib.dir/all] Error 2', 'make: *** [all] Error 2'])


@fixture()
def returns_failure_cpp_build(monkeypatch):
    'Returns a good run with status zero'
    monkeypatch.setattr('os.system', lambda cmd: write_logfile(cpp_build_log, cmd, 1))
    yield None


file_access_log = '''
Configured GCC from: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6
Configured AnalysisBase from: /usr/AnalysisBase/21.2.62/InstallArea/x86_64-slc6-gcc62-opt
-- The C compiler identification is GNU 6.2.0
-- The CXX compiler identification is GNU 6.2.0
-- Check for working C compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/gcc
-- Check for working C compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/gcc -- works
-- Detecting C compiler ABI info
-- Detecting C compiler ABI info - done
-- Detecting C compile features
-- Detecting C compile features - done
-- Check for working CXX compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/g++
-- Check for working CXX compiler: /opt/lcg/gcc/6.2.0binutils/x86_64-slc6/bin/g++ -- works
-- Detecting CXX compiler ABI info
-- Detecting CXX compiler ABI info - done
-- Detecting CXX compile features
-- Detecting CXX compile features - done
-- Found AnalysisBase: /usr/AnalysisBase/21.2.62/InstallArea/x86_64-slc6-gcc62-opt (version: 21.2.62)
-- Found AnalysisBaseExternals: /usr/AnalysisBaseExternals/21.2.62/InstallArea/x86_64-slc6-gcc62-opt (version: 21.2.62)
-- Setting ATLAS specific build flags
-- checker_gccplugins library not found
-- Package(s) in AnalysisBaseExternals: 19
-- Using the LCG modules without setting up a release
-- Package(s) in AnalysisBase: 173
-- Looking for pthread.h
-- Looking for pthread.h - found
-- Looking for pthread_create
-- Looking for pthread_create - not found
-- Check if compiler accepts -pthread
-- Check if compiler accepts -pthread - yes
-- Found Threads: TRUE
-- Configuring ATLAS project with name "UserAnalysis" and version "1.0.0"
-- Using build type: RelWithDebInfo
-- Using platform name: x86_64-slc6-gcc62-opt
-- Unit tests will be built by default
-- Found 1 package(s)
-- Using the LCG modules without setting up a release
-- Considering package 1 / 1
-- No package filtering rules read
-- Configuring the build of package: analysis
-- Number of packages configured: 1
-- Time for package configuration: 0 seconds
-- Using the LCG modules without setting up a release
-- Including the packages from project AnalysisBase - 21.2.62...
-- Including the packages from project AnalysisBaseExternals - 21.2.62...
-- Using the LCG modules without setting up a release
-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/packages.txt
-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/compilers.txt
-- Generated file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/ReleaseData
-- Generating external environment configuration
-- Using the LCG modules without setting up a release
-- Writing runtime environment to file: /home/atlas/rel/build/x86_64-slc6-gcc62-opt/env_setup.sh
-- Configuring done
-- Generating done
-- Build files have been written to: /home/atlas/rel/build
Scanning dependencies of target queryDictDictGen
[ 11%] Generating queryDictReflexDict.cxx
[ 11%] Built target queryDictDictGen
Scanning dependencies of target UserAnalysisRootMapMerge
[ 22%] Built UserAnalysisRootMapMerge
[ 22%] Built target UserAnalysisRootMapMerge
Scanning dependencies of target Package_analysis_tests
[ 22%] Built target Package_analysis_tests
Scanning dependencies of target atlas_tests
[ 22%] Built target atlas_tests
Scanning dependencies of target analysisScriptsInstall
[ 33%] Generating ../x86_64-slc6-gcc62-opt/bin/ATestRun_eljob.py
[ 33%] Built target analysisScriptsInstall
Scanning dependencies of target analysisLib
[ 44%] Building CXX object analysis/CMakeFiles/analysisLib.dir/Root/query.cxx.o
/home/atlas/rel/source/analysis/Root/query.cxx: In member function 'virtual StatusCode query::execute()':
/home/atlas/rel/source/analysis/Root/query.cxx:2467:18: warning: 'if_else_result2977' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Lpz3119 = if_else_result2977;
            ~~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2465:18: warning: 'if_else_result2968' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Lpt3118 = if_else_result2968;
            ~~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2463:19: warning: 'if_else_result2959' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Lphi3117 = if_else_result2959;
            ~~~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2461:19: warning: 'if_else_result2950' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Leta3116 = if_else_result2950;
            ~~~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2459:17: warning: 'if_else_result2940' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Lz3115 = if_else_result2940;
            ~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2457:17: warning: 'if_else_result2930' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Ly3114 = if_else_result2930;
            ~~~~~~~~^~~~~~~~~~~~~~~~~~~~
/home/atlas/rel/source/analysis/Root/query.cxx:2455:17: warning: 'if_else_result2920' may be used uninitialized in this function [-Wmaybe-uninitialized]
            _Lx3113 = if_else_result2920;
            ~~~~~~~~^~~~~~~~~~~~~~~~~~~~
[ 55%] Linking CXX shared library ../x86_64-slc6-gcc62-opt/lib/libanalysisLib.so
Detaching debug info of libanalysisLib.so into libanalysisLib.so.dbg
[ 55%] Built target analysisLib
Scanning dependencies of target analysisHeaderInstall
[ 66%] Generating ../x86_64-slc6-gcc62-opt/include/analysis
[ 66%] Built target analysisHeaderInstall
Scanning dependencies of target analysisJobOptInstall
[ 66%] Built target analysisJobOptInstall
Scanning dependencies of target queryDict
[ 77%] Building CXX object analysis/CMakeFiles/queryDict.dir/CMakeFiles/queryDictReflexDict.cxx.o
[ 88%] Linking CXX shared library ../x86_64-slc6-gcc62-opt/lib/libqueryDict.so
Detaching debug info of libqueryDict.so into libqueryDict.so.dbg
[ 88%] Built target queryDict
Scanning dependencies of target Package_analysis
[100%] Built package analysis
analysis: Package build succeeded
[100%] Built target Package_analysis
xAOD::Init                INFO    Environment initialised for data access
SampleHandler with 1 files
Sample:name=ANALYSIS,tags=()
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000028.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000034.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000038.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000041.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000042.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000043.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000044.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000059.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000064.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000065.pool.root.1
root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000066.pool.root.1


Running sample: ANALYSIS
EventLoop                INFO    Opening file root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1
TNetXNGFile::Open         ERROR   [ERROR] Server responded with an error: [3011] Unable to open /atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1; no such file or directory

/build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/SampleHandler/Root/ToolsOther.cxx:62:exception: failed to open file: root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1
EventLoop                ERROR   /build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/EventLoop/Root/MessageCheck.cxx:35 (void EL::Detail::report_exception()): caught exception: /build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/SampleHandler/Root/ToolsOther.cxx:62:exception: failed to open file: root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1
EventLoop                ERROR   /build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/EventLoop/Root/Worker.cxx:580 (StatusCode EL::Worker::openInputFile(std::__cxx11::string)): failed to open file root://func-adl-server-xcache//atlas/rucio/mc16_13TeV:DAOD_EXOT15.17545528._000027.pool.root.1
EventLoop                ERROR   /build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/EventLoop/Root/Worker.cxx:496 (StatusCode EL::Worker::processEvents(EL::EventRange&)): Failed to call "openInputFile (eventRange.m_url)"
EventLoop                ERROR   /build1/atnight/localbuilds/nightlies/21.2/AnalysisBase/athena/PhysicsAnalysis/D3PDTools/EventLoop/Root/DirectWorker.cxx:93 (void EL::DirectWorker::run()): Failed to call "processEvents (eventRange)", throwing exception
EventLoop                INFO    worker finished
Traceback (most recent call last):
    File "../source/analysis/share/ATestRun_eljob.py", line 59, in <module>
    driver.submit( job, options.submission_dir )
Exception: void EL::Driver::submit(const EL::Job& job, const string& location) =>
    processEvents (eventRange) (C++ exception of type runtime_error)
'''


@fixture()
def returns_failure_file_access(monkeypatch):
    'Returns a good run with status zero'
    monkeypatch.setattr('os.system', lambda cmd: write_logfile(file_access_log, cmd, 1))
    yield None


def test_normal_run(rabbit_info, request_body, returns_successful):
    '''
    The C++ run completes as expected.
    '''
    # Setup
    rabbit_channel, rabbit_connection, method = rabbit_info

    # Run the job
    process_message("dummynode.node.edu", rabbit_channel, method, None, request_body, rabbit_connection)

    # Now check that everything we wanted to happen, happened.
    rabbit_channel.basic_ack.assert_called_once()
    rabbit_channel.basic_reject.assert_not_called()


def test_cpp_compile_error(rabbit_info, request_body, returns_failure_cpp_build):
    '''
    There is a C++ compile error. In this case we expect that we get a total failure.
    '''
    # Setup
    rabbit_channel, rabbit_connection, method = rabbit_info

    # Run the job
    process_message("dummynode.node.edu", rabbit_channel, method, None, request_body, rabbit_connection)

    # Now check that everything we wanted to happen, happened.
    rabbit_channel.basic_ack.assert_called_once()
    rabbit_channel.basic_reject.assert_not_called()


def test_file_access_error(rabbit_info, request_body, returns_failure_file_access):
    '''
    There is a file access error. In this case we want to requeue and run again.
    '''
    # Setup
    rabbit_channel, rabbit_connection, method = rabbit_info

    # Run the job
    process_message("dummynode.node.edu", rabbit_channel, method, None, request_body, rabbit_connection)

    # Now check that everything we wanted to happen, happened.
    # TODO: This is actually a fatal error it turns out.
    rabbit_channel.basic_ack.assert_called_once()
    rabbit_channel.basic_reject.assert_not_called()
    # rabbit_channel.basic_ack.assert_not_called()
    # rabbit_channel.basic_reject.assert_called_once()
