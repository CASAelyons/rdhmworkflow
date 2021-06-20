#!/usr/bin/env python

import sys
import os
#import pwd
#import time
#import logging
import requests
import json, time, socket, subprocess, pytz, certifi, urllib3
from pathlib import Path
from Pegasus.api import *
from datetime import datetime, timezone, timedelta
from argparse import ArgumentParser


class rdhmWorkflow(object):
    def __init__(self, inputfile, starttime, endtime):
        #times given in ISO format like so: 20180908T1815 (no seconds)
        self.inputfile = inputfile
        self.starttime = starttime
        self.endtime = endtime
    def generate_jobs(self):
        #some timestamp parsing
        startyyyy = int(self.starttime[0:4])
        startmo = int(self.starttime[4:6])
        startdy= int(self.starttime[6:8])
        starthh = int(self.starttime[9:11])
        startmm = int(self.starttime[11:13])
        startss = 0

        startdt = datetime(startyyyy, startmo, startdy, starthh, startmm, tzinfo=timezone.utc)
        startout = startdt + timedelta(minutes=1)

        endyyyy = int(self.endtime[0:4])
        endmo = int(self.endtime[4:6])
        enddy= int(self.endtime[6:8])
        endhh = int(self.endtime[9:11])
        endmm = int(self.endtime[11:13])
        endss = 0

        enddt = datetime(endyyyy, endmo, enddy, endhh, endmm, tzinfo=timezone.utc)
        endout = enddt + timedelta(minutes=1)

        tmpout = startout
        output_ts_arr = []

        while True:
            #file times are written out by the model like mmDDYYHHMMSS
            outts = tmpout.strftime("%m%d%Y%H%M%S")
            print(outts)
            output_ts_arr.append(outts)
            if tmpout == endout:
                break
            else:
                tmpout = tmpout + timedelta(minutes=1)
        
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_rdhm_wf-%s" % ts)
        wfpath = str(Path.home()) + '/rdhmworkflow/'
        local_storage_path = wfpath + 'output'
        local_scratch_path = wfpath + 'scratch'
        #shared_storage_path = '/nfs/shared/rdhm/output/'
        f_local_storage_path = "file://" + local_storage_path
        f_local_scratch_path = "file://" + local_scratch_path
        #f_shared_storage_path = "file://" + shared_storage_path
        sc = SiteCatalog()
        
        shared_scratch = Directory(Directory.SHARED_SCRATCH, path="/nfs/shared/pegasus/scratch")\
                .add_file_servers(FileServer("file:///nfs/shared/pegasus/scratch", Operation.ALL))

        local_storage = Directory(Directory.LOCAL_STORAGE, path=local_storage_path)\
                .add_file_servers(FileServer(f_local_storage_path, Operation.ALL))

        local_scratch = Directory(Directory.LOCAL_SCRATCH, path=local_scratch_path)\
                .add_file_servers(FileServer(f_local_scratch_path, Operation.ALL))

        #shared_storage = Directory(Directory.SHARED_STORAGE, path=shared_storage_path)\
        #        .add_file_servers(FileServer(f_shared_storage_path, Operation.ALL))

        local = Site("local")
        
        #local.add_directories(shared_scratch,local_storage,local_scratch, shared_storage)
        local.add_directories(shared_scratch,local_storage,local_scratch)

        exec_site = Site("condorpool")
        #exec_site.add_directories(shared_scratch, shared_storage)\
        exec_site.add_directories(shared_scratch)\
                .add_pegasus_profile(clusters_size=8)\
                .add_pegasus_profile(cores=2)\
                .add_pegasus_profile(memory=2048)\
                .add_pegasus_profile(style="condor")\
                .add_env(SEQEXEC_CPUS=2)\
                .add_condor_profile(universe="vanilla")\
                .add_pegasus_profile(auxillary_local="true")\
                .add_profiles(Namespace.PEGASUS)\
                .add_pegasus_profile(data_configuration="nonsharedfs")


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
            mounts=["/nfs/shared:/nfs/shared", "/nfs/shared:/srv/nfs/shared"]
        )
        
        rdhm_container2 = Container(
            name="rdhm_container2",
            container_type=Container.SINGULARITY,
            image="file:///nfs/shared/ldm/rdhm_singularity.simg",
            image_site="condorpool",
            mounts=["/nfs/shared:/nfs/shared"]
        )
        
        rdhm_transformation = Transformation(
            name="rdhm",
            site="condorpool",
            pfn="/opt/rdhm/bin/rdhm",
            container=rdhm_container
        )

        gz_transformation = Transformation(
            name="gunzip",
            site="condorpool",
            pfn="/bin/gunzip",
            container=rdhm_container2
        )

        asc_transformation = Transformation(
            name="xmrgtoasc",
            site="condorpool",
            pfn="/opt/rdhm/bin/xmrgtoasc",
            container=rdhm_container
        )

        netcdf_transformation = Transformation(
            name="xmrgtonetcdf",
            site="condorpool",
            pfn="/opt/xmrgtonetcdf/xmrgtonetcdf",
            container=rdhm_container2
        )

        tc = TransformationCatalog()\
            .add_containers(rdhm_container, rdhm_container2)\
            .add_transformations(rdhm_transformation, gz_transformation, asc_transformation, netcdf_transformation)
        
        props = Properties()
        props["pegasus.transfer.links"]="true"
        props["pegasus.transfer.bypass.input.staging"]="true"
        props["pegasus.integrity.checking"]="none"
        props["pegasus.mode"]="development"
        propfilepath = wfpath + 'pegasus.properties'

        with open(propfilepath, "w") as f:
            props.write(f)
        

        rdhm_job = Job(rdhm_transformation)\
            .add_args("-s", self.starttime, "-f", self.endtime, inputfile)\
            .add_inputs(inputfile)


        #add output files
        surfaceFlow_output_dir = "/nfs/shared/rdhm/output/surfaceFlow/"
        discharge_output_dir = "/nfs/shared/rdhm/output/discharge/"
        returnp_output_dir = "/nfs/shared/rdhm/output/returnp/"
        asc_output_dir = "/nfs/shared/rdhm/asc/"
        netcdf_output_dir = "/nfs/shared/rdhm/netcdf/"
        
        gzjob_arr = []

        ascjob_arr = []

        ncjob_arr = []
        

        for tstamp in output_ts_arr:
            #create names/files of gzipped output files from rdhm and add to job
            surfaceFlow_gzfname = surfaceFlow_output_dir + "surfaceFlow" + tstamp + "z.gz"
            discharge_gzfname = discharge_output_dir + "discharge" + tstamp + "z.gz"
            returnp_gzfname = returnp_output_dir + "returnp" + tstamp + "z.gz"
            surfaceFlow_gzfile = File(surfaceFlow_gzfname)
            discharge_gzfile = File(discharge_gzfname)
            returnp_gzfile = File(returnp_gzfname)
            rdhm_job.add_outputs(surfaceFlow_gzfile, discharge_gzfile, returnp_gzfile, stage_out=False)
            
            #create names/files of unzipped files
            #surfaceFlow_fname = surfaceFlow_output_dir + "surfaceFlow" + tstamp + "z"
            #discharge_fname = discharge_output_dir + "discharge" + tstamp + "z"
            #returnp_fname = returnp_output_dir + "returnp" + tstamp + "z"
            surfaceFlow_fname = "surfaceFlow" + tstamp + "z"
            discharge_fname = "discharge" + tstamp + "z"
            returnp_fname = "returnp" + tstamp + "z"
            surfaceFlow_file = File(surfaceFlow_fname)
            discharge_file = File(discharge_fname)
            returnp_file = File(returnp_fname)
            
            #create gunzip jobs 
            surfaceFlow_gunzip_job = Job(gz_transformation)\
                .add_args("-kfc", surfaceFlow_gzfname)\
                .add_inputs(surfaceFlow_gzfile)\
                .set_stdout(surfaceFlow_file)
            discharge_gunzip_job = Job(gz_transformation)\
                .add_args("-kfc", discharge_gzfname)\
                .add_inputs(discharge_gzfile)\
                .set_stdout(discharge_file)
            returnp_gunzip_job = Job(gz_transformation)\
                .add_args("-kfc", returnp_gzfname)\
                .add_inputs(returnp_gzfile)\
                .set_stdout(returnp_file)

            #add gunzip jobs to job arrays
            gzjob_arr.append(surfaceFlow_gunzip_job)
            gzjob_arr.append(discharge_gunzip_job)
            gzjob_arr.append(returnp_gunzip_job)
            
            #surfaceFlow_asc_fname = asc_output_dir + "surfaceFlow" + tstamp + "z.asc"
            #discharge_asc_fname = asc_output_dir + "discharge" + tstamp + "z.asc"
            #returnp_asc_fname = asc_output_dir + "returnp" + tstamp + "z.asc"
            
            surfaceFlow_asc_fname = "surfaceFlow" + tstamp + "z.asc"
            discharge_asc_fname = "discharge" + tstamp + "z.asc"
            returnp_asc_fname = "returnp" + tstamp + "z.asc"
            surfaceFlow_asc_file = File(surfaceFlow_asc_fname)
            discharge_asc_file = File(discharge_asc_fname)
            returnp_asc_file = File(returnp_asc_fname)
            
            #create xmrgtoasc jobs
            surfaceFlow_asc_job = Job(asc_transformation)\
                .add_args("-i", surfaceFlow_fname, "-o", surfaceFlow_asc_fname, "-f", "\"-5.3f\"")\
                .add_inputs(surfaceFlow_file)\
                .add_outputs(surfaceFlow_asc_file)

            discharge_asc_job = Job(asc_transformation)\
                .add_args("-i", discharge_fname, "-o", discharge_asc_fname, "-f", "\"-5.3f\"")\
                .add_inputs(discharge_file)\
                .add_outputs(discharge_asc_file)

            returnp_asc_job = Job(asc_transformation)\
                .add_args("-i", returnp_fname, "-o", returnp_asc_fname, "-f", "\"-5.3f\"")\
                .add_inputs(returnp_file)\
                .add_outputs(returnp_asc_file)

            #add xmrgtoasc jobs to job arrays
            ascjob_arr.append(surfaceFlow_asc_job)
            ascjob_arr.append(discharge_asc_job)
            ascjob_arr.append(returnp_asc_job)
            
            #create names/files of netcdf files
            #surfaceFlow_nc_fname = netcdf_output_dir + "surfaceFlow" + tstamp + "z.netcdf"
            #discharge_nc_fname = netcdf_output_dir + "discharge" + tstamp + "z.netcdf"
            #returnp_nc_fname = netcdf_output_dir + "returnp" + tstamp + "z.netcdf"
            #surfaceFlow_nc_file = File(surfaceFlow_nc_fname)
            #discharge_nc_file = File(discharge_nc_fname)
            #returnp_nc_file = File(returnp_nc_fname)
            
            #create xmrgtonetcdf jobs
            #surfaceFlow_nc_job = Job(netcdf_transformation)\
            #    .add_args("-i", surfaceFlow_fname, "-o", surfaceFlow_nc_fname)\
            #    .add_inputs(surfaceFlow_file)\
            #    .add_outputs(surfaceFlow_nc_file)

            #discharge_nc_job = Job(netcdf_transformation)\
            #    .add_args("-i", discharge_fname, "-o", discharge_nc_fname)\
            #    .add_inputs(discharge_file)\
            #    .add_outputs(discharge_nc_file)

            #returnp_nc_job = Job(netcdf_transformation)\
            #    .add_args("-i", returnp_fname, "-o", returnp_nc_fname)\
            #    .add_inputs(returnp_file)\
            #    .add_outputs(returnp_nc_file)
            
            #add xmrgtonetcdf jobs to job arrays
            #ncjob_arr.append(surfaceFlow_nc_job)
            #ncjob_arr.append(discharge_nc_job)
            #ncjob_arr.append(returnp_nc_job)

        #Add the various jobs

        #main rdhm job
        wf.add_jobs(rdhm_job)
        
        #unzip
        for gzjob in gzjob_arr:
            wf.add_jobs(gzjob)

        #wf.add_dependency(rdhm_job, children=gzjob_arr)
        #xmrgtoasc
        for ascjob in ascjob_arr:
            wf.add_jobs(ascjob)

        #xmrgtonetcdf
        #for ncjob in ncjob_arr:
        #    wf.add_jobs(ncjob)
        
        wf.add_site_catalog(sc)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)

        wffilepath = wfpath + 'workflow.yml'
        wf.write(wffilepath)

        try:
            wf.plan(conf=propfilepath, dir=wfpath, cluster=['horizontal'], submit=True)
            wf.wait()
            wf.analyze()
            wf.statistics()
        except PegasusClientError as e:
            print(e.output)
    
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

