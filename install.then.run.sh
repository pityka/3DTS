#!/usr/bin/env bash

echo "needs yum"
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum -y install sbt git java-1.8.0
echo -e "2" | sudo /usr/sbin/alternatives --config java # configure to run on newer java-1.8.0 install

git clone git@github.com:humanlongevity/3DTS.git 
cd saturation 
git checkout public 
dep=$(ls dependencies/) 
for i in $dep; do cd dependencies/$i && sbt -batch publishLocal && cd ../../ ; done 
mkdir lib 
wget -O lib/jdistlib.jar https://downloads.sourceforge.net/project/jdistlib/jdistlib-0.4.5-bin.jar?r=&ts=1502307860&use_mirror=netcologne 

sbt -batch stage 

mkdir input  
mkdir data 

cat << EOF > input/conf 
akka.http.client.parsing.max-content-length = infinite
akka.http.host-connection-pool.client.idle-timeout = infinite 
tasks.fileservice.storageURI = ./data/
hosts.RAM=120000
hosts.numCPU=16 

uniprotKb = input/uniprot.gz
gencodeGTF = input/gencode.v26lift37.annotation.gtf.gz 
gencodeTranscripts = input/gencode.v26lift37.pc_transcripts.fa.gz 
gencodeMetadataXrefUniprot = input/gencode.v26lift37.metadata.SwissProt.gz 

gnomadGenome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/genomes/gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz"
gnomadExome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/exomes/gnomad.exomes.r2.0.1.sites.vcf.gz"

gnomadExomeCoverage = input/exome.coverage.concat.txt
gnomadGenomeCoverage = input/genome.coverage.concat.txt
EOF

wget -O input/uniprot.gz  'http://www.uniprot.org/uniprot/?sort=&desc=&compress=yes&query=proteome:UP000005640%20reviewed:yes&fil=&force=yes&format=txt' 

wget -O input/gencode.v26lift37.annotation.gtf.gz ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.annotation.gtf.gz 
wget -O input/gencode.v26lift37.pc_transcripts.fa.gz ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.pc_transcripts.fa.gz 
wget -O input/gencode.v26lift37.metadata.SwissProt.gz ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.metadata.SwissProt.gz 

wget -O input/genome.coverage.all.tar https://data.broadinstitute.org/gnomAD/release-170228/genomes/coverage/genome.coverage.all.tar 
wget -O input/exome.coverage.all.tar  https://data.broadinstitute.org/gnomAD/release-170228/exomes/coverage/exome.coverage.all.tar

mkdir tmp 

for i in $(tar tf input/genome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/genome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/genome.coverage.concat.txt; done
for i in $(tar tf input/exome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/exome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/exome.coverage.concat.txt; done

target/universal/stage/bin/saturation -Dconfig.file=input/conf -J-Xmx115G -Djava.io.tmpdir=tmp/
