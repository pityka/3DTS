3DTS: A Three-Dimensional Tolerance Score informed by human genetic diversity
=============================================================================
Sequence variation data of the human proteome can be used to analyze 3-dimensional 
(3D) protein structures to derive functional insights. This program maps observed
missense variation data to 3D structures, uses an underlying genetic model to 
estimate expectation, and produces 3D Tolerance Scores (3DTS) for genetic loci in
3D protein space. Details of the model are available through this code and are 
described in detail in the manuscript listed below (see Citation). 

Browse precomputed scores
-------------------------
To browse precomputed scores please head over to http://protc-app.labtelenti.org .
On this website you can retrieve and visualize the scores on the precomputed PDB and 
SwissModel structures.

---

The remaining part of this document describes how to build and use this software 
to recompute the scores.

Quick Start Using AWS EC2
-------------------------
1. Launch an r4.4xlarge EC2 instance (Amazon Linux AMI) with a 600GB EBS volume.
2. Download install.then.run.sh and run from the EBS volume. Please note that EC2/ftp issues may require manual download of files from GenCode (see Comments in install.then.run.sh).
~~~
./install.then.run.sh > log
~~~

Output
------
After the script finishes (~1 day), 3DTS scores will have been produced using:
1. Genomic annotations from GenCode.v26lift37
2. Genomic variation using gnomAD exomes and genomes data
3. Protein structure information from the PDB
4. Feature information from the current release of UniprotKB

The most relevant outputs will be the 3DTS scores and 3DTS feature definitions (filenames may vary slightly):
3DTS scores: saturation/data/depletion3-7/full.gencode.v26lift37.annotation.gtf.gz.genome.json.gz.variationdata.json.gz.5.0.-52800447..json.gz.gencode.v26lift37.annotation.gtf.gz.genome.json.gz.-1067519786.json.gz
3DTS feature defintions: saturation/data/structuralcontextfromfeatures/5.0.-52800447..json.gz 

Advanced Usage and Development
------------------------------

Building and Running
--------------------
1. Copy the jdistlib distribution jar to lib/ from http://jdistlib.sourceforge.net/
2. Install sbt (e.g. `brew install sbt`)
3. Publish to the local ivy repository all projects in the `dependencies/` folder
~~~
for i in $(ls dependencies/); do cd dependencies/$i && sbt publishLocal && cd ../../; done
~~~
4. From the root folder: `sbt packageZipTarball` 
5. Move the packaged application located in `target/universal/saturation-0.1-SNAPSHOT.tgz` to a machine with 120G RAM and ~600Gb disk. (e.g., EC2 instance type r4.4xlarge) 
6. Edit the configuration file (see Configuration section below)
7. Make sure the machine you deploy to has internet connection. The software will download cif files from https://files.rcsb.org/. 
8. Unzip the packaged application and run with 
~~~
bin/saturation -Dconfig.file=path/to/config -J-Xmx115G -Djava.io.tmpdir=path/to/tmp
~~~

Configuration
-------------
### Configuration keys related to the environment
~~~
akka.http.client.parsing.max-content-length = infinite
akka.http.host-connection-pool.client.idle-timeout = infinite 

# s3 or filesystem path where result and intermediate files will be written
tasks.fileservice.storageURI =  

hosts.RAM=120000
hosts.numCPU=16 # or whatever is convenient
~~~
### Configuration keys related to input files

The values of the keys should be http, https s3 URLs (s3 recommended), or filesystem paths.

~~~
uniprotKb = uniprot-all.txt.gz // Compressed Text formatted file for SwissProt (Reviewed) portion of Uniprot for Proteome UP000005640
gencodeGTF = gencode.v26lift37.annotation.gtf.gz // Comprehensive gene annotation for GRCh37
gencodeTranscripts = gencode.v26lift37.pc_transcripts.fa.gz // Protein-coding transcript sequences for GRCh37
gencodeMetadataXrefUniprot = gencode.v26lift37.metadata.SwissProt.gz // Cross-reference between GRCh37 and SwissProt

gnomadGenome = gnomad.genomes.r2.0.1.sites.coding.autosomes.vcf.gz // gnomAD Genome variants
gnomadExome = gnomad.exomes.r2.0.1.sites.vcf.gz // gnomAD Exome variants

# genome coverage files from the Gnomad browser should be concatenated 
# and header line removed
# e.g. `for i in $(tar tf genome.coverage.all.tar | grep -v tbi ) ; do tar xOf genome.coverage.all.tar $i ; done | gunzip -c | grep -v '#'  > genome.coverage.concat.txt`

gnomadExomeCoverage = exome.coverage.concat.txt // gnomAD Exome Coverage
gnomadGenomeCoverage = genome.coverage.concat.txt // gnomAD Genome Coverage
~~~

### Configuration file format
https://github.com/typesafehub/config 

Citation
--------
Manuscript under consideration. Submission available on bioRxiv: http://www.biorxiv.org/content/early/2017/08/29/182287

License
-------
The 3DTS Software Code (the "Code") is made available by Human Longevity, Inc. ("HLI") 
on a non-exclusive, non-sublicensable, non-transferable basis solely for non-commercial 
academic research use. Commercial use of the Code is expressly prohibited. If you would 
like to obtain a license to the Code for commercial use, please contact HLI at 
bizdev@humanlongevity.com. HLI MAKES NO REPRESENTATIONS OR WARRANTIES WHATSOEVER, EITHER 
EXPRESS OR IMPLIED, WITH RESPECT TO THE CODE PROVIDED HEREUNDER. IMPLIED WARRANTIES OF 
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE WITH RESPECT TO CODE ARE EXPRESSLY 
DISCLAIMED. THE CODE IS FURNISHED "AS IS" AND "WITH ALL FAULTS" AND DOWNLOADING OR USING 
THE CODE IS UNDERTAKEN AT YOUR OWN RISK. TO THE FULLEST EXTENT ALLOWED BY APPLICABLE LAW, 
IN NO EVENT SHALL HLI BE LIABLE, WHETHER IN CONTRACT, TORT, WARRANTY, OR UNDER ANY STATUTE 
OR ON ANY OTHER BASIS FOR SPECIAL, INCIDENTAL, INDIRECT, PUNITIVE, MULTIPLE OR CONSEQUENTIAL 
DAMAGES SUSTAINED BY YOU OR ANY OTHER PERSON OR ENTITY ON ACCOUNT OF USE OR POSSESSION OF THE 
CODE, WHETHER OR NOT FORESEEABLE AND WHETHER OR NOT HLI HAS BEEN ADVISED OF THE POSSIBILITY 
OF SUCH DAMAGES, INCLUDING WITHOUT LIMITATION DAMAGES ARISING FROM OR RELATED TO LOSS OF USE, 
LOSS OF DATA, DOWNTIME, OR FOR LOSS OF REVENUE, PROFITS, GOODWILL, BUSINESS OR OTHER FINANCIAL LOSS.
