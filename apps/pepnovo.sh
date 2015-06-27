#!/bin/bash
cd /home/ec2-user/proteinApps/pepnovo #Models are taken from folder Models which should be located in current position
rm -f -r inputPartialFile.mgf
while read line
do
  echo "$line" >> inputPartialFile.mgf
done < "${1:-/dev/stdin}"
./PepNovo_bin -file inputPartialFile.mgf -model CID_IT_TRYP

