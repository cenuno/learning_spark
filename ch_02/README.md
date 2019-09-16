# Chapter 2: Introduction to Data Analysis with Scala and Spark

We'll be using the Spark Standalone Cluster, which is launched using the
following commands from the Terminal:

```bash
# note: you can replace the 1 with however many threads to run.
#       replacing 1 with * will match the number of cores
#       available on your machine
spark-shell --master local[1]
```

This will launch the `spark-shell`, which is a REPL (read-eval-print loop)
for the Scala language that also has some Spark-specific extensions.
