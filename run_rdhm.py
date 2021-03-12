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


class rtmaWorkflow(object):
    def __init__(self, inputfile):
        self.inputfile = inputfile

    def generate_jobs(self):
        
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_rdhm_wf-%s" % ts)

        sc = SiteCatalog()
        
        shared_scratch = Directory(Directory.SHARED_SCRATCH, path="/nfs/shared/rdhm/scratch")\
                .add_file_servers(FileServer("file:///nfs/shared/rdhm/scratch", Operation.ALL))

        #container_location = Directory(Directory.SHARED_STORAGE, path="/nfs/shared/ldm")\
        #        .add_file_servers(FileServer("file:///nfs/shared/ldm", Operation.ALL))

        local_storage = Directory(Directory.LOCAL_STORAGE, "/home/ldm/rdhmworkflow/output")\
                .add_file_servers(FileServer("file:///home/ldm/rdhmworkflow/output", Operation.ALL))
        
        #local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
        local = Site("local")

        #local.add_directories(shared_scratch,local_storage, container_location)
        local.add_directories(shared_scratch,local_storage)

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
        props.write()
        
        rdhm_job = Job(rdhm_transformation)\
            .add_args(inputfile)\
            .add_inputs(inputfile)

        wf.add_jobs(rdhm_job)

        wf.add_site_catalog(sc)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        
        try:
            wf.plan(submit=True)
            wf.wait()
            wf.analyze()
            wf.statistics()
        except PegasusClientError as e:
            print(e.output)
        
    
    def generate_workflow(self):
        # Generate dax
        self.generate_jobs()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = ArgumentParser(description="RDHM Workflow")
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input netcdf file", required=True)

    args = parser.parse_args()
    inputfile = args.inputfile

    workflow = rdhmWorkflow(inputfile)
    workflow.generate_workflow()

