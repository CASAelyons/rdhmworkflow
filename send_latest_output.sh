#!/bin/sh

cd /nfs/shared/rdhm; tar -cf /nfs/shared/ldm/rdhm_output.tar `for l in \`ls -d *\`; do ls output/$l/*z.gz | tail -1; done`  `for l in \`ls -d *\`; do ls output/$l/current* ; done`; /home/ldm/bin/pqinsert -f EXP -p rdhm_output.tar /nfs/shared/ldm/rdhm_output.tar; rm /nfs/shared/ldm/rdhm_output.tar;
