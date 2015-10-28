#!/usr/bin/python

"""
cluster_subscribe_count
"""

import sys
import time
import logging

from argparse import ArgumentParser

import rediscluster

# configuration parameters
HOST = ['localhost:6379']
CHANNEL = 'chat'

# connection to Redis
r = None
args = None

#
#   __main__
#

try:
    # parse the command line arguments
    parser = ArgumentParser(description=__doc__)

    # add a way to turn on color debugging
    parser.add_argument("--host", type=str, nargs='+',
        help="redis host:port (default: %default)",
        default=HOST,
        )

    # add an option to override the database
    parser.add_argument('--channel', type=str,
        help="channel name (default: %default)",
        default=CHANNEL,
        )

    # now parse the arguments
    args = parser.parse_args()

    startup_nodes = []
    for host_port in args.host:
        if ':' not in host_port:
            host = host_port
            port = '6379'
        else:
            host, port = host_port.split(':')

        startup_nodes.append({'host': host, 'port': port})

    # connect to redis
    r = rediscluster.StrictRedisCluster(
            startup_nodes=startup_nodes,
            decode_responses=True,
            )

    # create an object for managing the subscription
    p = r.pubsub(ignore_subscribe_messages=True)

    # subscribe to the channel
    p.subscribe(args.channel)

    # start with no knowledge of messages
    message_id = None
    missed_message_count = 0

    # listen for messages
    for msg in p.listen():
        # print out the data
        if msg['type'] == 'message':
            current_message_id = int(msg['data'])

            if (message_id is not None) and (current_message_id != message_id + 1):
                sys.stdout.write("%d..%d\n" % (
                    message_id + 1,
                    current_message_id - 1,
                    ))
                missed_message_count += current_message_id - message_id - 1

            message_id = current_message_id

except Exception, e:
    sys.exit(1)

finally:
    sys.exit(0)

