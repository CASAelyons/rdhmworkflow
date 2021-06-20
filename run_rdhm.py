#!/usr/bin/env python

import sys
import os
#import pwd
#import time
import logging
import requests
import json, time, socket, subprocess, pytz, certifi, urllib3
from pathlib import Path
from Pegasus.api import *
from datetime import datetime
from argparse import ArgumentParser

logging.basicConfig(level=logging.INFO)
class rdhmWorkflow(object):
    def __init__(self, inputfile, starttime, endtime):
        #times given in ISO format like so: 20180908T1815 (no seconds)
        self.inputfile = inputfile
        self.starttime = starttime
        self.endtime = endtime
    def generate_jobs(self):
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_rdhm_wf-%s" % ts)
        wfpath = str(Path.home()) + '/rdhmworkflow/'
        local_storage_path = wfpath + 'output'
        local_scratch_path = wfpath + 'scratch'
        f_local_storage_path = "file://" + local_storage_path
        f_local_scratch_path = "file://" + local_scratch_path
        sc = SiteCatalog()
        
        shared_scratch = Directory(Directory.SHARED_SCRATCH, path="/nfs/shared/pegasus/scratch")\
                .add_file_servers(FileServer("file:///nfs/shared/pegasus/scratch", Operation.ALL))

        local_storage = Directory(Directory.LOCAL_STORAGE, path=local_storage_path)\
                .add_file_servers(FileServer(f_local_storage_path, Operation.ALL))

        local_scratch = Directory(Directory.LOCAL_SCRATCH, path=local_scratch_path)\
                .add_file_servers(FileServer(f_local_scratch_path, Operation.ALL))

        local = Site("local")

        local.add_directories(shared_scratch,local_storage,local_scratch)

        exec_site = Site("condorpool")
        exec_site.add_directories(shared_scratch)\
                .add_pegasus_profile(clusters_size=32)\
                .add_pegasus_profile(cores=4)\
                .add_pegasus_profile(data_configuration="nonsharedfs")\
                .add_pegasus_profile(memory=2048)\
                .add_pegasus_profile(style="condor")\
                .add_condor_profile(universe="vanilla")\
                .add_pegasus_profile(auxillary_local="true")\
                .add_profiles(Namespace.PEGASUS)

        sc.add_sites(local, exec_site)

        inputfile = File("Realtime_RSRT2_CASA_container.card")
        #inputfile = File(self.inputfile)
        
        rc = ReplicaCatalog()\
             .add_replica("condorpool", inputfile, "/nfs/shared/rdhm/input/Realtime_RSRT2_CASA_container.card")
        
        rdhm_container = Container(
            name="rdhm_container",
            container_type=Container.SINGULARITY,
            image="file:///nfs/shared/ldm/rdhm_singularity.simg",
            image_site="condorpool",
            bypass_staging=False,
            mounts=["/nfs/shared:/nfs/shared"]
        )
        
        rdhm_transformation = Transformation(
            name="rdhm",
            site="condorpool",
            pfn="/opt/rdhm/bin/rdhm",
            bypass_staging=False,
            container=rdhm_container
        )
        
        tc = TransformationCatalog()\
            .add_containers(rdhm_container)\
            .add_transformations(rdhm_transformation)
        
        props = Properties()
        props["pegasus.transfer.links"]="true"
        props["pegasus.transfer.bypass.input.staging"]="true"
        props["pegasus.integrity.checking"]="none"
        propfilepath = wfpath + 'pegasus.properties'

        with open(propfilepath, "w") as f:
            props.write(f)
        

        rdhm_job = Job(rdhm_transformation)\
            .add_args("-s", self.starttime, "-f", self.endtime, inputfile)\
            .add_inputs(inputfile)

        wf.add_jobs(rdhm_job)
        wf.add_site_catalog(sc)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)

        wffilepath = wfpath + 'workflow.yml'
        wf.write(wffilepath)

        try:
            wf.plan(conf=propfilepath, dir=wfpath, submit=True)
        except PegasusClientError as e:
            print(e.output)

        #wf.wait()
        #wf.analyze()
        #wf.statistics()
        
    
    def generate_workflow(self):
        # Generate dax
        self.generate_jobs()

if __name__ == '__main__':
    parser = ArgumentParser(description="RDHM Workflow")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input card (configfile)", required=True)
    parser.add_argument("-s", "--starttime", metavar="START_TIME", type=str, help="Start time in ISO format-> YYYYMMDDTHHMM (use UTC)", required=True)
    parser.add_argument("-f", "--endtime", metavar="END_TIME", type=str, help="End time in ISO format-> YYYYMMDDTHHMM (use UTC)", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    starttime = args.starttime
    endtime = args.endtime

    workflow = rdhmWorkflow(inputfile, starttime, endtime)
    workflow.generate_workflow()

