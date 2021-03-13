#!/usr/bin/env python

import sys
import os
#import pwd
import numpy as N
#import time
#import logging
#import requests
import math
import json
import geojson as gj
#import time, socket, subprocess, pytz, certifi, urllib3
#import gzip
#from pathlib import Path
#from datetime import datetime
from argparse import ArgumentParser


class returnp_to_geojson(object):
    def __init__(self, inputfile, outputfile, nxfile, nyfile):
        self.inputfile = inputfile
        self.outputfile = outputfile
        self.nxfile = nxfile
        self.nyfile = nyfile
    def convert_to_geojson(self):
        ncols=236
        nrows=164

        if os.path.exists(self.nxfile) and os.path.exists(self.nyfile):
            nx=N.loadtxt(self.nxfile, delimiter=" ", unpack=False)
            ny=N.loadtxt(self.nyfile, delimiter=" ", unpack=False)
        else:
            print("Cannot locate latitude file and/or longitude file.  Exiting...")
            sys.exit(0)
        
        returnp=N.ones((nrows,ncols),dtype=float)

        if os.path.exists(self.inputfile):
            outputD=N.loadtxt(self.inputfile, skiprows=6, unpack=False)
        else:
            print("Cannot locate returnp file.  Exiting...")
            sys.exit(0)

        outputDf=N.flipud(outputD)
        os.remove(self.inputfile)
        
        polygon_feats = []
        
        for i in range(nrows-1,-1,-1):
            for j in range(ncols):
                if (outputDf[i,j]<0):
                    returnp[i,j]=0
                else: 
                    returnp[i,j]=outputDf[i,j]
                
        for ii in range(nrows-1,-1,-1):
            for jj in range(ncols): 
                if (jj<235 and ii>0):
                    if (returnp[ii,jj]>1):
                        pol = gj.Polygon([[(-1.*ny[ii,jj],nx[ii,jj]),(-1.*ny[ii,jj+1],nx[ii,jj+1]),(-1.*ny[ii-1,jj+1],nx[ii-1,jj+1]),(-1.*ny[ii-1,jj],nx[ii-1,jj]),(-1.*ny[ii,jj], nx[ii,jj])]])
                        
                        if (returnp[ii,jj]>1 and returnp[ii,jj]<=2):
                            polcolor = '#4cecec'
                        elif (returnp[ii,jj]>2 and returnp[ii,jj]<=3):
                            polcolor = '#44c6f0'
                        elif (returnp[ii,jj]>3 and returnp[ii,jj]<=4):
                            polcolor = '#429afb'
                        elif (returnp[ii,jj]>4 and returnp[ii,jj]<=5):
                            polcolor = '#3431fd'
                        elif (returnp[ii,jj]>5 and returnp[ii,jj]<=6):
                            polcolor = '#40f600'       
                        elif (returnp[ii,jj]>6 and returnp[ii,jj]<=7):
                            polcolor = '#3ada0b' 
                        elif (returnp[ii,jj]>7 and returnp[ii,jj]<=8):
                            polcolor = '#2eb612'    
                        elif (returnp[ii,jj]>8 and returnp[ii,jj]<=10):
                            polcolor = '#2a8a0f'   
                        elif (returnp[ii,jj]>10 and returnp[ii,jj]<=12):
                            polcolor = '#f8f915'
                        elif (returnp[ii,jj]>12 and returnp[ii,jj]<=14):
                            polcolor = '#e9d11c'  
                        elif (returnp[ii,jj]>14 and returnp[ii,jj]<=16):
                            polcolor = '#dcb11f' 
                        elif (returnp[ii,jj]>16 and returnp[ii,jj]<=18):
                            polcolor = '#bd751f'    
                        elif (returnp[ii,jj]>18 and returnp[ii,jj]<=20):
                            polcolor = '#f39a9c'     
                        elif (returnp[ii,jj]>20 and returnp[ii,jj]<=25):
                            polcolor = '#f23a43'
                        elif (returnp[ii,jj]>25 and returnp[ii,jj]<=30):
                            polcolor = '#da1622'
                        elif (returnp[ii,jj]>30 and returnp[ii,jj]<=35):
                            polcolor = '#a90c1b'
                        elif (returnp[ii,jj]>35 and returnp[ii,jj]<=40):
                            polcolor = '#fa31ff'
                        elif (returnp[ii,jj]>40 and returnp[ii,jj]<=50):
                            polcolor = '#d32ada'
                        elif (returnp[ii,jj]>50 and returnp[ii,jj]<=60):
                            polcolor = '#9f1fa3'
                        elif (returnp[ii,jj]>60 and returnp[ii,jj]<=75):
                            polcolor = '#751678'
                        elif (returnp[ii,jj]>75 and returnp[ii,jj]<=100):
                            polcolor = '#ffffff'
                        elif (returnp[ii,jj]>100 and returnp[ii,jj]<=200):
                            polcolor = '#c1bdff'
                        elif (returnp[ii,jj]>200 and returnp[ii,jj]<=500):
                            polcolor = '#c5ffff'
                        elif (returnp[ii,jj]>500):
                            polcolor = '#fcfec0'
                            
                        feature = gj.Feature(geometry=pol, properties={"color": polcolor, "dataType": "hydrology", "productType": "return period", "value": returnp[ii,jj], "units": "years"})
                        polygon_feats.append(feature)

        
        features = []
        features.extend(polygon_feats)
        featCollection = gj.FeatureCollection(features)
        dumpFC = gj.dumps(featCollection, sort_keys=True)
        try:
            of = open(self.outputfile, 'w')
        except OSError:
            print("Could not open output file for writing: " + self.outputfile)  
            print("exiting...")
            sys.exit(0)
            
        of.write(dumpFC)
        of.close
        sys.exit(1)

    def run_conversion(self):
        self.convert_to_geojson()

if __name__ == '__main__':
#    logging.basicConfig(level=logging.DEBUG)

    parser = ArgumentParser(description="returnp_to_geojson")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input returnp xmrg file", required=True)
    parser.add_argument("-o", "--outputfile", metavar="OUTPUT_FILE", type=str, help="Path to output returnp geojson file", required=True)
    parser.add_argument("-x", "--nxfile", metavar="LONGITUDE_FILE", type=str, help="Path to input longitude file (usually nx.txt)", required=True)
    parser.add_argument("-y", "--nyfile", metavar="LATITUDE_FILE", type=str, help="Path to input latitude file (usually ny.txt)", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    outputfile = args.outputfile
    nxfile = args.nxfile
    nyfile = args.nyfile
    workflow = returnp_to_geojson(inputfile, outputfile, nxfile, nyfile)
    workflow.run_conversion()

