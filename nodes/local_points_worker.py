import sys
import argparse
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from workers.local_points import LocalPointsWorker

def main(port):

    worker = LocalPointsWorker(port)

    worker.run()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                    description='Local points worker NBA',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            '--port',
            type=int,
            default=6666,
            help='The port to receive data from dispatcher'
    )
    args = parser.parse_args()

    main(args.port)

