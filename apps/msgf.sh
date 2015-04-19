#!/bin/bash
#cd /home/ec2-user #Models are taken from folder Models which should be located in current position
rm -f -r test.mgf
while read line
do
  echo "$line" >> test.mgf
done < "${1:-/dev/stdin}"
java -Xmx256M -jar MSGFPlus.jar -s smallinputfile.mgf -d testdb.fasta -o testoutput.mzid
java -cp MSGFPlus.jar edu.ucsd.msjava.ui.MzIDToTsv -i testoutput.mzid -o testoutput.tsv
cat testoutput.tsv