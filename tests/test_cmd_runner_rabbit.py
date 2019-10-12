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


# Do a C++ compile failure
# Do a file not found failure
