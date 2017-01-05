#!/usr/bin/env python

import os
import argparse
import numpy as np

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prints random numbers to file')
    parser.add_argument('--length', dest='length', type=int,
                   default='length',
                   help='Number of random numbers you would like to be written to file')
    args = parser.parse_args()

    numbers = np.random.rand(args.length)
    sys.exit()
    np.savetxt('{}/numbers.csv'.format(os.getcwd()), numbers)
