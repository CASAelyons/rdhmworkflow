#!/bin/sh
ymdstr=`date -d "-2days" +%Y%m%d`
mdystr=`date -d "-2days" +%m%d%Y`

rm -rf /home/ldm/rdhmworkflow/ldm/pegasus/*-${ymdstr}*
rm -rf /nfs/shared/rdhm/output/*/*[a-z]${mdystr}*
rm -rf /nfs/shared/rdhm/geojson/*[a-z]${mdystr}*
