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
                            polcolor = 'cyan'
                        elif (returnp[ii,jj]>2 and returnp[ii,jj]<=3):
                            polcolor = 'royalblue'
                        elif (returnp[ii,jj]>3 and returnp[ii,jj]<=4):
                            polcolor = 'darkblue'
                        elif (returnp[ii,jj]>4 and returnp[ii,jj]<=5):
                            polcolor = 'lawngreen'
                        elif (returnp[ii,jj]>5 and returnp[ii,jj]<=6):
                            polcolor = 'forestgreen'       
                        elif (returnp[ii,jj]>6 and returnp[ii,jj]<=7):
                            polcolor = 'darkgreen' 
                        elif (returnp[ii,jj]>7 and returnp[ii,jj]<=8):
                            polcolor = 'yellow'    
                        elif (returnp[ii,jj]>8 and returnp[ii,jj]<=9):
                            polcolor = 'orange'   
                        elif (returnp[ii,jj]>9 and returnp[ii,jj]<=10):
                            polcolor = 'darkorange'
                        elif (returnp[ii,jj]>10 and returnp[ii,jj]<=11):
                            polcolor = 'orangered'  
                        elif (returnp[ii,jj]>11 and returnp[ii,jj]<=12):
                            polcolor = 'red' 
                        elif (returnp[ii,jj]>12 and returnp[ii,jj]<=13):
                            polcolor = 'darkred'    
                        elif (returnp[ii,jj]>13 and returnp[ii,jj]<=14):
                            polcolor = 'mediumvioletred'     
                        elif (returnp[ii,jj]>14 and returnp[ii,jj]<=15):
                            polcolor = 'purple'     
                        elif (returnp[ii,jj]>15):
                            polcolor = 'white'
                            
                        feature = gj.Feature(geometry=pol, properties={"color": polcolor, "dataType": "hydrology", "productType": "return period", "value": returnp[ii,jj]}, "units": "years")
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

