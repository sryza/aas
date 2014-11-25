Simple Spark Maven Project
==============

A demonstration of a simple Spark project that uses Maven for building.  The app simply counts the
number of lines in a text file.

To build a jar:

    mvn package
    
To build a jar against CDH:

    mvn package -Pcdh

To run on a cluster with Spark installed:

    spark-submit --class com.cloudera.datascience.MyApp --master local \
      target/simplesparkproject-0.0.1-SNAPSHOT.jar <input file>

To run a REPL that can reference the objects and classes defined in this project:

    spark-shell --jars target/simplesparkproject-0.0.1-SNAPSHOT.jar --master local

The `--master local` argument means that the application will run in a single local process.  If
the cluster is running a Spark standalone cluster manager, you can replace it with
`--master spark://<master host>:<master port>`. If the cluster is running YARN, you can replace it
with `--master yarn`.

To pass configuration options on the command line, use the `--conf` option, e.g.
`--conf spark.serializer=org.apache.spark.serializer.KryoSerializer`.
