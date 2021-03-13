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


class surfaceFlow_to_geojson(object):
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

        surfaceFlow=N.ones((nrows,ncols),dtype=float)

        if os.path.exists(self.inputfile):
            outputD=N.loadtxt(self.inputfile, skiprows=6, unpack=False)
        else:
            print("Cannot locate surfaceFlow file.  Exiting...")
            sys.exit(0)

        outputDf=N.flipud(outputD)
        os.remove(self.inputfile)
        
        polygon_feats = []
        
        for i in range(nrows-1,-1,-1):
            for j in range(ncols):
                if (outputDf[i,j]<0):
                    surfaceFlow[i,j]=0
                else: 
                    surfaceFlow[i,j]=(outputDf[i,j]/25.4)*60
                
        for ii in range(nrows-1,-1,-1):
            for jj in range(ncols): 
                if (jj<235 and ii>0):
                    if (surfaceFlow[ii,jj] > 0.04):
                        pol = gj.Polygon([[(-1.*ny[ii,jj],nx[ii,jj]),(-1.*ny[ii,jj+1],nx[ii,jj+1]),(-1.*ny[ii-1,jj+1],nx[ii-1,jj+1]),(-1.*ny[ii-1,jj],nx[ii-1,jj]),(-1.*ny[ii,jj], nx[ii,jj])]])
                        
                        if (surfaceFlow[ii,jj]>0.04 and surfaceFlow[ii,jj]<=0.5):
                            polcolor = 'cyan'
                        elif (surfaceFlow[ii,jj]>0.5 and surfaceFlow[ii,jj]<=1):
                            polcolor = 'royalblue'
                        elif (surfaceFlow[ii,jj]>1 and surfaceFlow[ii,jj]<=1.5):
                            polcolor = 'darkblue'
                        elif (surfaceFlow[ii,jj]>1.5 and surfaceFlow[ii,jj]<=2):
                            polcolor = 'lawngreen'
                        elif (surfaceFlow[ii,jj]>2 and surfaceFlow[ii,jj]<=2.5):
                            polcolor = 'forestgreen'       
                        elif (surfaceFlow[ii,jj]>2.5 and surfaceFlow[ii,jj]<=3):
                            polcolor = 'darkgreen' 
                        elif (surfaceFlow[ii,jj]>3 and surfaceFlow[ii,jj]<=3.5):
                            polcolor = 'yellow'    
                        elif (surfaceFlow[ii,jj]>3.5 and surfaceFlow[ii,jj]<=4):
                            polcolor = 'orange'   
                        elif (surfaceFlow[ii,jj]>4 and surfaceFlow[ii,jj]<=4.5):
                            polcolor = 'darkorange'
                        elif (surfaceFlow[ii,jj]>4.5 and surfaceFlow[ii,jj]<=5):
                            polcolor = 'orangered'  
                        elif (surfaceFlow[ii,jj]>5 and surfaceFlow[ii,jj]<=5.5):
                            polcolor = 'red' 
                        elif (surfaceFlow[ii,jj]>5.5 and surfaceFlow[ii,jj]<=6):
                            polcolor = 'darkred'    
                        elif (surfaceFlow[ii,jj]>6 and surfaceFlow[ii,jj]<=6.5):
                            polcolor = 'mediumvioletred'     
                        elif (surfaceFlow[ii,jj]>6.5 and surfaceFlow[ii,jj]<=7):
                            polcolor = 'purple'     
                        elif (surfaceFlow[ii,jj]>7):
                            polcolor = 'white'
                            
                        feature = gj.Feature(geometry=pol, properties={"color": polcolor, "dataType": "hydrology", "productType": "runoff rate", "value": surfaceFlow[ii,jj]})
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

    parser = ArgumentParser(description="surfaceFlow_to_geojson")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input surfaceFLow xmrg file", required=True)
    parser.add_argument("-o", "--outputfile", metavar="OUTPUT_FILE", type=str, help="Path to output surfaceFlow geojson file", required=True)
    parser.add_argument("-x", "--nxfile", metavar="LONGITUDE_FILE", type=str, help="Path to input longitude file (usually nx.txt)", required=True)
    parser.add_argument("-y", "--nyfile", metavar="LATITUDE_FILE", type=str, help="Path to input latitude file (usually ny.txt)", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    outputfile = args.outputfile
    nxfile = args.nxfile
    nyfile = args.nyfile
    workflow = surfaceFlow_to_geojson(inputfile, outputfile, nxfile, nyfile)
    workflow.run_conversion()

