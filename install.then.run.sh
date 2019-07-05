#!/usr/bin/env bash

# COMPILE 

echo "needs yum"
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum -y install sbt git java-1.8.0
echo -e "2" | sudo /usr/sbin/alternatives --config java # configure to run on newer java-1.8.0 install

git clone https://github.com/pityka/3DTS.git
cd 3DTS
git checkout wip
dep=$(ls dependencies/)
for i in $dep; do cd dependencies/$i && sbt -batch publishLocal && cd ../../ ; done
mkdir lib
wget -O lib/jdistlib.jar https://downloads.sourceforge.net/project/jdistlib/jdistlib-0.4.5-bin.jar?r=&ts=1502307860&use_mirror=netcologne

cd dependencies
git clone https://github.com/biasmv/pv
cp pv/bio-pv.min.js ../src/main/resources/public/
cd ..

sbt -batch stage

# DOWNLOAD DATA AND WRITE CONFIG

mkdir input
mkdir data

cat << EOF > input/conf
akka.http.client.parsing.max-content-length = infinite
akka.http.host-connection-pool.client.idle-timeout = infinite
tasks.fileservice.storageURI = ./data/
hosts.RAM=120000
hosts.numCPU=16

akka {
  loggers = ["akka.event.slf4j.Slf4jLogger"]
  loglevel = "DEBUG"
  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
}

uniprotKb = input/uniprot.gz
gencodeGTF = input/gencode.v26lift37.annotation.gtf.gz
gencodeTranscripts = input/gencode.v26lift37.pc_transcripts.fa.gz
gencodeMetadataXrefUniprot = input/gencode.v26lift37.metadata.SwissProt.gz

# Server issues with storage.googleapis.com
#gnomadGenome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/genomes/gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz"
gnomadGenome = "https://data.broadinstitute.org/gnomAD/release-170228/genomes/vcf/gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz"
#gnomadExome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/exomes/gnomad.exomes.r2.0.1.sites.vcf.gz"
gnomadExome = "https://data.broadinstitute.org/gnomAD/release-170228/exomes/vcf/gnomad.exomes.r2.0.1.sites.vcf.gz"

gnomadExomeCoverage = input/exome.coverage.concat.txt
gnomadGenomeCoverage = input/genome.coverage.concat.txt

fasta = input/human_g1k_v37.fasta
fai = input/human_g1k_v37.fasta.fai

tasks.skipContentHashVerificationAfterCache = true
tasks.fileservice.folderFileStorageCompleteFileCheck = false

gnomadWGSCoverage = [
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr1.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr2.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr3.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr4.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr5.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr6.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr7.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr8.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr9.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr10.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr11.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr12.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr13.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr14.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr15.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr16.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr17.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr18.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr19.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr20.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr21.coverage.txt.gz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/coverage/genomes/gnomad.genomes.r2.0.2.chr22.coverage.txt.gz"

	]

gnomadWGSVCF = [
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr1.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr2.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr3.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr4.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr5.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr6.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr7.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr8.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr9.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr10.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr11.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr12.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr13.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr14.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr15.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr16.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr17.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr18.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr19.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr20.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr21.vcf.bgz"
	"https://storage.googleapis.com/gnomad-public/release/2.0.2/vcf/genomes/gnomad.genomes.r2.0.2.sites.chr22.vcf.bgz"
	]

swissModelMetaData = "https://swissmodel.expasy.org/repository/download/core_species/9606_meta.tar.gz"

radius = 5.0
EOF

wget -O input/uniprot.gz  'http://www.uniprot.org/uniprot/?sort=&desc=&compress=yes&query=proteome:UP000005640%20reviewed:yes&fil=&force=yes&format=txt'

# The following three gencode files may need to be downloaded manually and transferred to the instance
# If they are downloaded manually, put them in the same directory as the install.then.run.sh
wget -O input/gencode.v26lift37.annotation.gtf.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.annotation.gtf.gz
wget -O input/gencode.v26lift37.pc_transcripts.fa.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.pc_transcripts.fa.gz
wget -O input/gencode.v26lift37.metadata.SwissProt.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.metadata.SwissProt.gz


wget -O input/genome.coverage.all.tar https://data.broadinstitute.org/gnomAD/release-170228/genomes/coverage/genome.coverage.all.tar
wget -O input/exome.coverage.all.tar  https://data.broadinstitute.org/gnomAD/release-170228/exomes/coverage/exome.coverage.all.tar

wget -O input/human_g1k_v37.fasta.gz ftp://ftp.ncbi.nlm.nih.gov/1000genomes/ftp/technical/reference/human_g1k_v37.fasta.gz
wget -O input/human_g1k_v37.fasta.fai ftp://ftp.ncbi.nlm.nih.gov/1000genomes/ftp/technical/reference/human_g1k_v37.fasta.fai

gunzip input/human_g1k_v37.fasta.gz

mkdir tmp

for i in $(tar tf input/genome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/genome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/genome.coverage.concat.txt done
for i in $(tar tf input/exome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/exome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/exome.coverage.concat.txt; done

rm input/genome.coverage.all.tar
rm input/exome.coverage.all.tar

# RUN

target/universal/stage/bin/saturation -Dconfig.file=input/conf -J-Xmx115G -Djava.io.tmpdir=tmp/ -Dfile.encoding=UTF-8
