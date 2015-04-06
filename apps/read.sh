#!/bin/bash
cd /home/ec2-user #Models are taken from folder Models which should be located in current position
rm -f -r test.mgf
while read line
do
  echo "$line" >> test.mgf
done < "${1:-/dev/stdin}"
./PepNovo_bin -file test.mgf -model CID_IT_TRYP

