# DySparkExtensionOS
Open sourcing internal extension for Apache Spark we've developed in DynamicYield.
## Description
- Extension tries to solve problem of loading persisted data and providing partitioner
- Usually when you save partitioned RDD to hdfs/s3 and want to load it next day(or on next iteration) in different spark context the information regarding partitioner is lost
- The extension is based on [Spark PR by Imran Rashid which wasn't merged into master branch](https://github.com/apache/spark/pull/4449)
- _Important Detail_ : You should disable input splits first - usually you'll take your InputFormat, extend it and override isSplittable method e.g.
```
public class AvroKeyValueInputFormatNotSplittable<K, V> extends AvroKeyValueInputFormat<K, V> {
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
```
- for basic usage example see AssumedPartitionsPairRDDFunctionsSuite.scala

## Compatability
- current code works with spark 1.5.1
- in general each version of spark may require changes to this code

## Declaimer
We are not scala experts, so please try not to vomit from our scala code. In general our spark code base is in java 7 and we write in scala only on special cases when we need add java api wrapper over scala api or to implement some extension to core spark. Thanks for understanding and suggestions are welcome!
