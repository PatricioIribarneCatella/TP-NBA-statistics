import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from middleware.proxy import WorkerReducerProxy

def main(xsub_port, xpub_port):

    proxy = WorkerReducerProxy(xsub_port, xpub_port)

    proxy.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Match summary proxy NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--wport',
            type=int,
            default=6666,
            help='The port to receive data from workers'
    )
    parser.add_argument(
            '--rport',
            type=int,
            default=8888,
            help='The port to send processed data to reducers'
    )
    args = parser.parse_args()

    main(args.wport, args.rport)

