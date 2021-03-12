#!/usr/bin/env python

import sys
import os
import pwd
#import time
#import logging
#import requests
import json, geojson, time, socket, subprocess, pytz, certifi, urllib3
import gzip
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser


class discharge_to_geojson(object):
    def __init__(self, inputfile, outputfile):
        self.inputfile = inputfile
        self.outputfile = outputfile
    def convert_to_geojson(self):
        with gzip.open(inputfile, 'r') as gzfile:
            fc = gzfile.read()
        print fc
        sys.exit()
    def run_conversion(self):
        self.convert_to_geojson()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = ArgumentParser(description="discharge_to_geojson")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input discharge xmrg file", required=True)
    parser.add_argument("-o", "--starttime", metavar="OUTPUT_FILE", type=str, help="Path to output discharge geojsonfile", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    outputfile = args.outputfile
    workflow = discharge_to_geojson(inputfile, outputfile)
    workflow.run_conversion()

