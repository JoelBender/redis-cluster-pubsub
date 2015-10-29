#!/usr/bin/python

"""
cluster_publish_count
"""

import time
import re

from argparse import ArgumentParser

import rediscluster

# configuration parameters
HOST = ['127.0.0.1:6379']
CHANNEL = 'chat'
COUNT = 100
SLEEP = 1.0

# connection to Redis
r = None
args = None

# IPv4 pattern
IPv4 = re.compile(r"^\d+[.]\d+[.]\d+[.]\d+$")

#
#   __main__
#

def main():
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

    # add an option to override the database
    parser.add_argument('count', type=int,
        help="count (default: %default)",
        default=COUNT,
        )

    # add an option to override the database
    parser.add_argument('sleep', type=float,
        help="sleep (default: %default)",
        default=SLEEP,
        )

    # now parse the arguments
    args = parser.parse_args()

    startup_nodes = []
    for host_port in args.host:
        if ':' not in host_port:
            host = host_port
            port = 6379
        else:
            host, port = host_port.split(':')
            port = int(port)

        if not IPv4.match(host):
            raise ValueError, "host specification must be an IPv4 address"

        startup_nodes.append({'host': host, 'port': port})

    # connect to redis
    r = rediscluster.StrictRedisCluster(
            startup_nodes=startup_nodes,
            decode_responses=True,
            )

    # publish short messages
    message_id = 1
    while (args.count == 0) or (message_id <= args.count):
        # publish the counter, ignore the result
        r.publish(args.channel, str(message_id))

        # sleep for a while
        if args.sleep:
            time.sleep(args.sleep)

        # update the counter
        message_id += 1

if __name__ == "__main__":
    main()

