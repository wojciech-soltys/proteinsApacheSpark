#!/bin/bash
cd /home/ec2-user/proteinApps/msgf
rm -f -r inputPartialFile.mgf
while read line
do
  echo "$line" >> inputPartialFile.mgf
done < "${1:-/dev/stdin}"
java -Xmx256M -jar MSGFPlus.jar -s inputPartialFile.mgf -d testdb.fasta -o outputPartialFile.mzid
java -cp MSGFPlus.jar edu.ucsd.msjava.ui.MzIDToTsv -i outputPartialFile.mzid -o outputPartialFile.tsv
cat outputPartialFile.tsv