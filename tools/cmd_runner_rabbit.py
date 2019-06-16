# Build and run the executable as part of a rabbit runner.
import sys
import pika
import json
import os
import shutil

# WARNING:
# 1) This expects /cache to already be defined.
# 2) This needs to run inside an ATLAS container. Which means python 2.

def process_message(ch, method, properties, body):
    'Process each message and run the C++ for it.'

    # Make sure nothing from the previous job is sitting there waiting.
    if os.path.exists('/home/atlas/rel'):
        shutil.rmtree('/home/atlas/rel')

    # Unpack the incoming message.
    r = json.loads(body)

    hash = r['hash']
    main_script = r['main_script']
    input_files = r['files']

    with open('filelist.txt', 'w') as f:
        for f_name in input_files:
            f.write(f_name + '\n')
    
    os.system(os.path.join('/cache', hash, main_script))

    # This is as far as we go.
    ch.basic_ack(delivery_tag=method.delivery_tag)

def listen_to_queue(rabbit_node):
    'Get the various things downloaded and running'

    # Connect and setup the queues we will listen to and push once we've done.
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node))
    channel = connection.channel()
    channel.queue_declare(queue='run_cpp')

    channel.basic_consume(queue='run_cpp', on_message_callback=process_message, auto_ack=False)

    # We are setup. Off we go. We'll never come back.
    channel.start_consuming()

if __name__ == '__main__':
    bad_args = len(sys.argv) != 2
    if bad_args:
        print "Usage: python cmd_runner_rabbit.py <rabbit-mq-node-address>"
    else:
        listen_to_queue (sys.argv[1])
