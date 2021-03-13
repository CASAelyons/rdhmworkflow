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


class discharge_to_geojson(object):
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

        discharge=N.ones((nrows,ncols),dtype=float)
        A=N.ones((nrows,ncols),dtype=float)

        if os.path.exists(self.inputfile):
            outputD=N.loadtxt(self.inputfile, skiprows=6, unpack=False)
        else:
            print("Cannot locate discharge file.  Exiting...")
            sys.exit(0)

        outputDf=N.flipud(outputD)
        os.remove(self.inputfile)
        
        polygon_feats = []
        
        for i in range(nrows-1,-1,-1):
            for j in range(ncols):
                if (outputDf[i,j]<=0):
                    A[i,j]=0.1
                else:
                    A[i,j]=(outputDf[i,j])*35.3147        #CMS to CFS
                discharge[i,j]=math.log(A[i,j],10)
                
        for ii in range(nrows-1,-1,-1):
            for jj in range(ncols): 
                if (jj<235 and ii>0):
                    if (discharge[ii,jj]>0.5):
                        pol = gj.Polygon([[(-1.*ny[ii,jj],nx[ii,jj]),(-1.*ny[ii,jj+1],nx[ii,jj+1]),(-1.*ny[ii-1,jj+1],nx[ii-1,jj+1]),(-1.*ny[ii-1,jj],nx[ii-1,jj]),(-1.*ny[ii,jj], nx[ii,jj])]])
                        
                        if (discharge[ii,jj]>0.477 and discharge[ii,jj]<=1):
                            polcolor = 'cyan'
                        elif (discharge[ii,jj]>1 and discharge[ii,jj]<=1.30):
                            polcolor = 'royalblue'
                        elif (discharge[ii,jj]>1.30 and discharge[ii,jj]<=1.7):
                            polcolor = 'darkblue'
                        elif (discharge[ii,jj]>1.7 and discharge[ii,jj]<=2):
                            polcolor = 'lawngreen'
                        elif (discharge[ii,jj]>2 and discharge[ii,jj]<=2.2):
                            polcolor = 'forestgreen'       
                        elif (discharge[ii,jj]>2.2 and discharge[ii,jj]<=2.5):
                            polcolor = 'darkgreen' 
                        elif (discharge[ii,jj]>2.5 and discharge[ii,jj]<=2.8):
                            polcolor = 'yellow'    
                        elif (discharge[ii,jj]>2.8 and discharge[ii,jj]<=2.9):
                            polcolor = 'orange'   
                        elif (discharge[ii,jj]>2.9 and discharge[ii,jj]<=3):
                            polcolor = 'darkorange'
                        elif (discharge[ii,jj]>3 and discharge[ii,jj]<=3.2):
                            polcolor = 'orangered'  
                        elif (discharge[ii,jj]>3.2 and discharge[ii,jj]<=3.3):
                            polcolor = 'red' 
                        elif (discharge[ii,jj]>3.3 and discharge[ii,jj]<=3.4):
                            polcolor = 'darkred'    
                        elif (discharge[ii,jj]>3.4 and discharge[ii,jj]<=3.5):
                            polcolor = 'mediumvioletred'     
                        elif (discharge[ii,jj]>3.5 and discharge[ii,jj]<=3.556):
                            polcolor = 'purple'     
                        elif (discharge[ii,jj]>3.556):
                            polcolor = 'white'
                            
                        feature = gj.Feature(geometry=pol, properties={"color": polcolor, "dataType": "hydrology", "productType": "streamflow", "value": A[ii,jj], "units": "CFS"})
                        polygon_feats.append(feature)

        
        features = []
        features.extend(polygon_feats)
        featCollection = gj.FeatureCollection(features)
        featCollection['id'] = self.outputfile
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

    parser = ArgumentParser(description="discharge_to_geojson")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input discharge xmrg file", required=True)
    parser.add_argument("-o", "--outputfile", metavar="OUTPUT_FILE", type=str, help="Path to output discharge geojson file", required=True)
    parser.add_argument("-x", "--nxfile", metavar="LONGITUDE_FILE", type=str, help="Path to input longitude file (usually nx.txt)", required=True)
    parser.add_argument("-y", "--nyfile", metavar="LATITUDE_FILE", type=str, help="Path to input latitude file (usually ny.txt)", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    outputfile = args.outputfile
    nxfile = args.nxfile
    nyfile = args.nyfile
    workflow = discharge_to_geojson(inputfile, outputfile, nxfile, nyfile)
    workflow.run_conversion()

