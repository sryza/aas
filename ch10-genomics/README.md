ENCODE TF Prediction
====================

First we'll choose a set of samples and transcription factors.

```python
import requests

# DNase-seq; Homo sapiens; immortalized cell line; narrowPeak data
dnase_url = "https://www.encodeproject.org/search/?type=experiment&assay_term_name=DNase-seq&replicates.library.biosample.donor.organism.scientific_name=Homo%20sapiens&replicates.library.biosample.biosample_type=immortalized%20cell%20line&files.file_format=narrowPeak&format=json&&limit=all"
dnase_data = requests.get(dnase_url).json()
dnase_samples = set([exp['biosample_term_name'] for exp in dnase_data['@graph']])

chipseq_url = "https://www.encodeproject.org/search/?type=experiment&assay_term_name=ChIP-seq&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens&replicates.library.biosample.biosample_type=immortalized+cell+line&files.file_format=narrowPeak&format=json&&limit=all"
chipseq_data = requests.get(chipseq_url).json()
chipseq_tfs = {}
for exp in chipseq_data['@graph']:
    chipseq_tfs.setdefault(exp['target.label'], set()).add(exp['biosample_term_name'])

intersected = dict([(tf, samples & dnase_samples) for (tf, samples) in chipseq_tfs.iteritems()])

sorted(intersected.iteritems(), key=lambda kv: -len(intersected[kv[0]]))[:10]
```

We'll do the experiment on CTCF.  Let's download the relevant data for a few cell lines:

```
GM12878
K562
BJ
HEK293
H54
HepG2
```

Now we download all the relevant data sets.

DNase data:

```bash
hadoop fs -mkdir /user/ds/genomics/dnase
curl -s -L "https://www.encodeproject.org/files/ENCFF001UVC/@@download/ENCFF001UVC.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/GM12878.DNase.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001UWQ/@@download/ENCFF001UWQ.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/K562.DNase.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001WEI/@@download/ENCFF001WEI.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/BJ.DNase.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001UVQ/@@download/ENCFF001UVQ.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/HEK293.DNase.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001SOM/@@download/ENCFF001SOM.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/H54.DNase.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001UVU/@@download/ENCFF001UVU.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/dnase/HepG2.DNase.narrowPeak
```

GENCODE data:

```bash
curl -s -L "ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_human/release_18/gencode.v18.annotation.gtf.gz" | gunzip | hadoop fs -put - /user/ds/genomics/gencode.v18.annotation.gtf
```

ChIP-seq data for CTCF:

```bash
hadoop fs -mkdir /user/ds/genomics/chip-seq
curl -s -L "https://www.encodeproject.org/files/ENCFF001VED/@@download/ENCFF001VED.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/GM12878.ChIP-seq.CTCF.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001VMZ/@@download/ENCFF001VMZ.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/K562.ChIP-seq.CTCF.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001XMU/@@download/ENCFF001XMU.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/BJ.ChIP-seq.CTCF.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001XQU/@@download/ENCFF001XQU.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/HEK293.ChIP-seq.CTCF.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001USC/@@download/ENCFF001USC.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/H54.ChIP-seq.CTCF.narrowPeak
curl -s -L "https://www.encodeproject.org/files/ENCFF001XRC/@@download/ENCFF001XRC.bed.gz" | gunzip | hadoop fs -put - /user/ds/genomics/chip-seq/HepG2.ChIP-seq.CTCF.narrowPeak
```

phyloP data:

```bash
hadoop fs -mkdir /user/ds/genomics/phylop_text
for i in $(seq 1 22); do
    echo "chr$i.phyloP46way.wigFix.gz"
    curl -s -L "http://hgdownload-test.cse.ucsc.edu/goldenPath/hg19/phyloP46way/vertebrate/chr$i.phyloP46way.wigFix.gz" | gunzip | adam-submit wigfix2bed | hadoop fs -put - "/user/ds/genomics/phylop_text/chr$i.phyloP46way.wigFix"
done
curl -s -L "http://hgdownload-test.cse.ucsc.edu/goldenPath/hg19/phyloP46way/vertebrate/chrX.phyloP46way.wigFix.gz" | gunzip | adam-submit wigfix2bed | hadoop fs -put - /user/ds/genomics/phylop_text/chrX.phyloP46way.wigFix
curl -s -L "http://hgdownload-test.cse.ucsc.edu/goldenPath/hg19/phyloP46way/vertebrate/chrY.phyloP46way.wigFix.gz" | gunzip | adam-submit wigfix2bed | hadoop fs -put - /user/ds/genomics/phylop_text/chrY.phyloP46way.wigFix
```

In a Spark shell, we then convert the PhyloP data from text to Parquet, to
improve performance:

```scala
// Convert phyloP data to Parquet for better performance; run once
sc.adamBEDFeatureLoad("/user/ds/genomics/phylop_text").adamSave("/user/ds/genomics/phylop")
```

hg19 genome data:

```bash
curl -s -L -O "http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.2bit"
```

The code for the TF binding site prediction data preparation can be found in `g
enomics/src/main/scala/com/cloudera/datascience/genomics/RunTFPrediction.scala`


Genome Variant Analysis
=======================

First download the 1000 Genome VCF data into HDFS like so (this can be
parallelized across the cluster):

    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr1.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr1.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr2.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr2.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr3.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr3.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr4.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr4.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr5.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr5.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr6.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr6.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr7.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr7.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr8.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr8.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr9.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr9.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr10.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr10.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr11.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr11.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr12.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr12.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr13.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr13.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr14.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr14.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr15.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr15.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr16.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr16.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr17.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr17.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr18.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr18.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr19.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr19.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr20.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr20.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr21.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr21.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
    curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chrX.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | hadoop fs -put - /user/ds/genomics/1kg/vcf/ALL.chrX.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf


Convert each file to parquet

    export SPARK_JAR_PATH=hdfs:///user/laserson/tmp/spark-assembly-1.2.1-hadoop2.4.0.jar
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr1.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr1
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr2.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr2
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr3.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr3
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr4.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr4
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr5.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr5
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr6.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr6
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr7.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr7
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr8.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr8
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr9.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr9
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr10.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr10
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr11.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr11
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr12.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr12
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr13.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr13
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr14.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr14
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr15.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr15
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr16.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr16
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr17.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr17
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr18.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr18
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr19.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr19
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr20.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr20
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr21.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr21
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr22
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G vcf2adam -coalesce 5 /user/ds/genomics/1kg/vcf/ALL.chrX.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chrX


Convert to ADAM

    export HADOOP_CONF_DIR=/etc/hadoop/conf
    export SPARK_HOME=/home/laserson/spark
    export SPARK_JAR_PATH=hdfs:///user/laserson/tmp/spark-assembly-1.2.1-hadoop2.4.0.jar
    adam/bin/adam-submit --conf spark.yarn.jar=$SPARK_JAR_PATH --master yarn-cluster --driver-memory 4G --num-executors 6 --executor-cores 12 --executor-memory 4G vcf2adam -onlyvariants /user/ds/genomics/exac/vcf/ExAC.r0.2.sites.vep.vcf /user/ds/genomics/exac/parquet





==================================

    adam/bin/adam-submit \
    transform \
    --conf spark.yarn.jar=hdfs:///user/laserson/tmp/spark-assembly-1.2.1-hadoop2.4.0.jar \
    --master yarn-cluster \
    --driver-memory 4G --num-executors 3 --executor-cores 2 --executor-memory 4G
    /user/ds/genomics/HG00103.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam \
    /user/ds/genomics/reads/HG00103-test-1