#!/usr/bin/env python

import logging

# Specify logging settings
logging.basicConfig(
    format='%(levelname)s: dagmanager - %(name)s : %(message)s')
logging_level_dict = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}


def setup_logger(cls, verbose):
    # Set up logger
    if verbose not in logging_level_dict:
        raise KeyError('Verbose option {} for {} not valid. Valid options are {}.'.format(
            verbose, cls.name, logging_level_dict.keys()))
    logger = logging.getLogger(cls.name)
    logger.setLevel(logging_level_dict[verbose])
    logger.debug('CondorExecutable {} initialized'.format(cls.name))

    return logger
