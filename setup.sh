#!/usr/bin/env bash

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

# Server issues with storage.googleapis.com
#gnomadGenome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/genomes/gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz"
gnomadGenome = "https://data.broadinstitute.org/gnomAD/release-170228/genomes/vcf/gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz"
#gnomadExome = "https://storage.googleapis.com/gnomad-public/release-170228/vcf/exomes/gnomad.exomes.r2.0.1.sites.vcf.gz"
gnomadExome = "https://data.broadinstitute.org/gnomAD/release-170228/exomes/vcf/gnomad.exomes.r2.0.1.sites.vcf.gz"

gnomadExomeCoverage = input/exome.coverage.concat.txt
gnomadGenomeCoverage = input/genome.coverage.concat.txt
EOF

wget -O input/uniprot.gz  'http://www.uniprot.org/uniprot/?sort=&desc=&compress=yes&query=proteome:UP000005640%20reviewed:yes&fil=&force=yes&format=txt'

# The following three gencode files may need to be downloaded manually and transferred to the instance
# If they are downloaded manually, put them in the same directory as the install.then.run.sh
wget -O input/gencode.v26lift37.annotation.gtf.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.annotation.gtf.gz
wget -O input/gencode.v26lift37.pc_transcripts.fa.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.pc_transcripts.fa.gz
wget -O input/gencode.v26lift37.metadata.SwissProt.gz ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_26/GRCh37_mapping/gencode.v26lift37.metadata.SwissProt.gz
mv ../gencode.v26lift37.annotation.gtf.gz input/gencode.v26lift37.annotation.gtf.gz
mv ../gencode.v26lift37.pc_transcripts.fa.gz input/gencode.v26lift37.pc_transcripts.fa.gz
mv ../gencode.v26lift37.metadata.SwissProt.gz input/gencode.v26lift37.metadata.SwissProt.gz


wget -O input/genome.coverage.all.tar https://data.broadinstitute.org/gnomAD/release-170228/genomes/coverage/genome.coverage.all.tar
wget -O input/exome.coverage.all.tar  https://data.broadinstitute.org/gnomAD/release-170228/exomes/coverage/exome.coverage.all.tar

mkdir tmp

for i in $(tar tf input/genome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/genome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/genome.coverage.concat.txt; done
for i in $(tar tf input/exome.coverage.all.tar | grep -v tbi ) ; do tar xOf input/exome.coverage.all.tar $i | gunzip -c | grep -v "#" >> input/exome.coverage.concat.txt; done

