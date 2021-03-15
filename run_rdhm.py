#!/usr/bin/env python

import sys
import os
import pwd
#import time
import logging
import requests
import json, geojson, time, socket, subprocess, pytz, certifi, urllib3
from pathlib import Path
from Pegasus.api import *
from datetime import datetime
from argparse import ArgumentParser


class rdhmWorkflow(object):
    def __init__(self, inputfile, starttime, endtime):
        #times given in ISO format like so: 20180908T1815 (no seconds)
        self.inputfile = inputfile
        self.starttime = starttime
        self.endtime = endtime
    def generate_jobs(self):
        logging.critical('In workflow\n')
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_rdhm_wf-%s" % ts)
        wfpath = str(Path.home()) + '/rdhmworkflow/'
        logging.critical('workflow path: ' + wfpath + '\n')
        
        local_storage_path = wfpath + 'output'
        local_scratch_path = wfpath + 'scratch'
        f_local_storage_path = "file://" + local_storage_path
        f_local_scratch_path = "file://" + local_scratch_path
        sc = SiteCatalog()
        
        shared_scratch = Directory(Directory.SHARED_SCRATCH, path="/nfs/shared/pegasus/scratch")\
                .add_file_servers(FileServer("file:///nfs/shared/pegasus/scratch", Operation.ALL))

        #container_location = Directory(Directory.SHARED_STORAGE, path="/nfs/shared/ldm")\
        #        .add_file_servers(FileServer("file:///nfs/shared/ldm", Operation.ALL))

        local_storage = Directory(Directory.LOCAL_STORAGE, path=local_storage_path)\
                .add_file_servers(FileServer(f_local_storage_path, Operation.ALL))

        local_scratch = Directory(Directory.LOCAL_SCRATCH, path=local_scratch_path)\
                .add_file_servers(FileServer(f_local_scratch_path, Operation.ALL))

        #local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
        local = Site("local")

        #local.add_directories(shared_scratch,local_storage, container_location)
        local.add_directories(shared_scratch,local_storage,local_scratch)

        #exec_site = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
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

        #exec_site.add_directories(shared_scratch, container_location)

        sc.add_sites(local, exec_site)

        inputfile = File("Realtime_RSRT2_CASA_container.card")
        #inputfile = self.inputfile
        
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
        #logging.critical('writing properties\n')
        propfilepath = wfpath + 'pegasus.properties'
        logging.critical('writing properties to:' + propfilepath + '\n')
        with open(propfilepath, "w") as f:
            props.write(f)
        
        logging.critical('creating job')
        rdhm_job = Job(rdhm_transformation)\
            .add_args("-s", self.starttime, "-f", self.endtime, inputfile)\
            .add_inputs(inputfile)
        logging.critical('adding jobs and catalogs\n')
        wf.add_jobs(rdhm_job)
        logging.critical('addjob\n')
        wf.add_site_catalog(sc)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        
        logging.critical('trying to plan\n')
        try:
            wf.plan(conf=propfilepath, dir=wfpath, verbose=3, submit=True)
            #wf.plan(conf=propfilepath, dir=wfpath, verbose=3)
        except PegasusClientError as e:
            print(e.output)
        logging.critical('planned\n')
            #wf.wait()
            #wf.analyze()
            #wf.statistics()
        
    
    def generate_workflow(self):
        # Generate dax
        self.generate_jobs()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename="/home/ldm/rdhmworkflow/perl/pegasusPy.log")
    logging.critical('Initiated!')
    parser = ArgumentParser(description="RDHM Workflow")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input card (configfile)", required=True)
    parser.add_argument("-s", "--starttime", metavar="START_TIME", type=str, help="Start time in ISO format-> YYYYMMDDTHHMM (use UTC)", required=True)
    parser.add_argument("-f", "--endtime", metavar="END_TIME", type=str, help="End time in ISO format-> YYYYMMDDTHHMM (use UTC)", required=True)
    args = parser.parse_args()
    inputfile = args.inputfile
    starttime = args.starttime
    endtime = args.endtime
    logging.critical('Calling workflow')
    workflow = rdhmWorkflow(inputfile, starttime, endtime)
    workflow.generate_workflow()

