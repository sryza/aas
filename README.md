Advanced Analytics with Spark Source Code
=========================================

Code to accompany [Advanced Analytics with Spark](http://shop.oreilly.com/product/0636920035091.do), 
by [Sandy Ryza](https://github.com/sryza), [Uri Laserson](https://github.com/laserson), 
[Sean Owen](https://github.com/srowen), and [Josh Wills](https://github.com/jwills).

[![Advanced Analytics with Spark](http://akamaicovers.oreilly.com/images/0636920035091/lrg.jpg)](http://shop.oreilly.com/product/0636920035091.do)

### Build

[Apache Maven](http://maven.apache.org/) 3.0.5+ and Java 7+ are required to build. From the root level of the project, run `mvn package` to compile artifacts into `target/` subdirectories beneath each chapter's directory.

### Data Sets

- Chapter 2: https://archive.ics.uci.edu/ml/machine-learning-databases/00210/
- Chapter 3: http://www-etud.iro.umontreal.ca/~bergstrj/audioscrobbler_data.html
- Chapter 4: https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/
- Chapter 5: http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html (do _not_ use http://www.sigkdd.org/kdd-cup-1999-computer-network-intrusion-detection as the copy has a corrupted line)
- Chapter 6: http://dumps.wikimedia.org/enwiki/20150112/enwiki-20150112-pages-articles-multistream.xml.bz2
- Chapter 7: ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/ (`*.gz`)
- Chapter 8: http://www.andresmh.com/nyctaxitrips/
- Chapter 9: (see `ch09-risk/data/download-all-symbols.sh` script)
- Chapter 10: ftp://ftp.ncbi.nih.gov/1000genomes/ftp/phase3/data/HG00103/alignment/HG00103.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
- Chapter 11: https://github.com/thunder-project/thunder/tree/v0.4.1/python/thunder/utils/data/fish/tif-stack

[![Build Status](https://travis-ci.org/sryza/aas.png?branch=master)](https://travis-ci.org/sryza/aas)
