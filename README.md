# DySparkExtensionOS
Open sourcing internal extension for Apache Spark we've developed in DynamicYield.
## Description
- Extension is about loading data from persistent storage with assuming partitions.
- Usually when you save partitioned RDD to hdfs/s3 and want to load it next day(or on next iteration),
the information regarding partitioner is lost.
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
- for basic usage example see AssumedPartitionsSuite test

