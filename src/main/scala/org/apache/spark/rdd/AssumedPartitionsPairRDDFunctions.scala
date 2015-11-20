/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.rdd
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, HashMap => JHashMap}
import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.DynamicVariable
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat,
  RecordWriter => NewRecordWriter}
import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

/**
 * @author igor
 */
class AssumedPartitionsPairRDDFunctions [K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with SparkHadoopMapReduceUtil
  with Serializable
{
    /**
   * Return an RDD that assumes the data in this RDD has been partitioned.  This can be useful
   * to create narrow dependencies to avoid shuffles.  This can be especially useful when loading
   * an RDD from HDFS, where that RDD was already partitioned when it was saved.  Note that when
   * loading a file from HDFS, you must ensure that the input splits are disabled with a custom
   * FileInputFormat.
   *
   * If verify == true, every record will be checked against the Partitioner.  If any record is not
   * in the correct partition, an exception will be thrown when the RDD is computed.
   *
   * If verify == false, and the RDD is not partitioned correctly, the behavior is undefined.  There
   * may be a runtime error, or there may simply be wildly inaccurate results with no warning.
   */
  def assumePartitionedBy(partitioner: Partitioner, verify: Boolean = true): RDD[(K,V)] = {
    new AssumedPartitionedRDD[K,V](self, partitioner, verify)
  }
}

object AssumedPartitionsPairRDDFunctions {
 implicit def apply[K,V](rdd : RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) : AssumedPartitionsPairRDDFunctions[K,V] = {
   new AssumedPartitionsPairRDDFunctions[K,V](rdd)
 }
}
