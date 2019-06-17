# Build and run the executable as part of a rabbit runner.
import sys
import pika
import json
import os
import shutil

# WARNING:
# 1) This expects /cache to already be defined.
# 2) This needs to run inside an ATLAS container. Which means python 2.

def process_message(xrootd_node, ch, method, properties, body):
    'Process each message and run the C++ for it.'

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
    print xrootd_file

    with open('filelist.txt', 'w') as f:
        for f_name in input_files:
            f.write(f_name + '\n')
    
    ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'running'}))
    os.system(os.path.join('/cache', code_hash, main_script) + " " + xrootd_file)
    ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'done'}))

    # And send that done file on too.
    ch.basic_publish(exchange='', routing_key='status_add_file', body=json.dumps({'hash':hash, 'file':output_file}))

    # This is as far as we go.
    ch.basic_ack(delivery_tag=method.delivery_tag)

def listen_to_queue(rabbit_node, xrootd_node):
    'Get the various things downloaded and running'

    # Connect and setup the queues we will listen to and push once we've done.
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node))
    channel = connection.channel()

    # We pull tasks off this guy.
    channel.queue_declare(queue='run_cpp')
    # Let them know about progress
    channel.queue_declare(queue='status_change_state')
    # Add files as they complete
    channel.queue_declare(queue='status_add_file')

    # Listen for work to show up.
    channel.basic_consume(queue='run_cpp', on_message_callback=lambda ch, method, properties, body:process_message(xrootd_node, ch, method, properties, body), auto_ack=False)

    # We are setup. Off we go. We'll never come back.
    channel.start_consuming()

if __name__ == '__main__':
    bad_args = len(sys.argv) != 3
    if bad_args:
        print "Usage: python cmd_runner_rabbit.py <rabbit-mq-node-address> <xrootd-results_node>"
    else:
        listen_to_queue (sys.argv[1], sys.argv[2])
