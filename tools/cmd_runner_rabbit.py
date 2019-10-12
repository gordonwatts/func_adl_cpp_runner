# Build and run the executable as part of a rabbit runner.
import sys
import pika
import json
import os
import shutil
import logging
import threading
import base64
import zipfile
import tempfile


def check_log_file_for_fatal_errors(lines):
    '''Return true if the log file contains a fatal error that means we should
    not be re-running this. Falls otherwise.
    '''
    return False


def process_message(xrootd_node, ch, method, properties, body, connection):
    '''
    Process each message and run the C++ for it.

    Arguments:
        xrootd_node     xrootd server to store results on
        ch              rabbit mq channel
        method          rabbit mq method
        properties      rabbit mq properties
        body            body of the incoming message (json)
        connection      rabbit mq connection
    '''

    # Make sure nothing from the previous job is sitting there waiting.
    if os.path.exists('/home/atlas/rel'):
        shutil.rmtree('/home/atlas/rel')

    # Unpack the incoming message.
    r = json.loads(body)

    hash = r['hash']
    code_hash = r['hash_source']
    main_script = r['main_script']
    input_files = r['files']
    output_file = r['output_file']
    xrootd_file = "root://" + xrootd_node + "//" + output_file
    logging.info('We are looking at an xrootd file: ' + xrootd_file)
    source_files = r['file_data']

    # Unpack the source files we are going to run against
    zip_filename = os.path.join(tempfile.tempdir, code_hash + '.zip')
    with open(zip_filename, 'wb') as zip_data:
        zip_data.write(base64.b64decode(source_files))
        zip_data.close()
        logging.info('Length of binary data we got: ' + str(len(source_files)))

    zip_output = os.path.join(tempfile.tempdir, code_hash + '_files')
    if not os.path.exists(zip_output):
        os.mkdir(zip_output)
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(zip_output)

    # Write the file list that we are to process
    with open('filelist.txt', 'w') as f:
        for f_name in input_files:
            f.write(f_name + '\n')
    log_file = os.path.join(tempfile.tempdir, code_hash + '.log')

    # Now run the thing.
    connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash': hash, 'phase': 'running'})))
    rtn_code = os.system('set -o pipefail; sh ' + zip_output + '/' + main_script + " " + xrootd_file + ' 2>&1 | tee ' + log_file)
    logging.info('Return code from run: ' + str(rtn_code))

    retry_message = False
    if rtn_code != 0:
        # First, do we need to re-try this crash or ont?
        with open(log_file) as f:
            content = f.read().splitlines()
        # Log the error message.
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='crashed_request',
                                                                    body=json.dumps({'hash': hash, 'message': 'while building and running xAOD', 'log': content})))

        # If it is fatal, then we move this job to crashed
        is_fatal = check_log_file_for_fatal_errors(content)
        if is_fatal:
            # Report the error and the log file.
            connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash': hash, 'phase': 'crashed'})))
        else:
            # We want the put the message back on the queue and have someone else try it out.
            retry_message = True
    else:
        # Update the status, and send the file on for use by the person that requested it.
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash': hash, 'phase': 'done'})))
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_add_file', body=json.dumps({'hash': hash, 'file': output_file, 'treename': r['treename']})))

    # This is as far as we go.
    if retry_message:
        connection.add_callback_threadsafe(lambda: ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True))
    else:
        connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=method.delivery_tag))


def start_message_processing_thread(xrootd_node, ch, method, properties, body, connection):
    ''' Starts a message processing in a new thread.
    This is so the msg recv loop doesn't need to remain busy.
    '''
    logging.debug('Firing off a thread processing.')
    t = threading.Thread(target=process_message, args=(xrootd_node, ch, method, properties, body, connection))
    t.start()
    logging.debug('done loading the thread up.')


def listen_to_queue(rabbit_node, xrootd_node, rabbit_user, rabbit_pass):
    'Get the various things downloaded and running'

    # Connect and setup the queues we will listen to and push once we've done.
    if rabbit_pass in os.environ:
        rabbit_pass = os.environ[rabbit_pass]
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node, credentials=credentials))
    channel = connection.channel()

    # We run pretty slowly, so make sure this guy doesn't keep too many.
    channel.basic_qos(prefetch_count=1)

    # We pull tasks off this guy.
    channel.queue_declare(queue='run_cpp')
    # Let them know about progress
    channel.queue_declare(queue='status_change_state')
    # Add files as they complete
    channel.queue_declare(queue='status_add_file')
    # Record a crash
    channel.queue_declare(queue='crashed_request')

    # Listen for work to show up.
    channel.basic_consume(queue='run_cpp', on_message_callback=lambda ch, method, properties, body: start_message_processing_thread(xrootd_node, ch, method, properties, body, connection), auto_ack=False)

    # We are setup. Off we go. We'll never come back.
    channel.start_consuming()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    bad_args = len(sys.argv) != 5
    if bad_args:
        print "Usage: python cmd_runner_rabbit.py <rabbit-mq-node-address> <xrootd-results_node> <rabbit-username> <rabbit-password>"
    else:
        listen_to_queue(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
