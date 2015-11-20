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
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext._
import org.apache.spark.Partitioner
import scala.reflect.ClassTag
import org.apache.spark.rdd.AssumedPartitionsPairRDDFunctions._
import org.apache.spark.rdd.PairRDDFunctions._
import java.io.Closeable
import java.util
import java.util.{Map => JMap}
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import com.google.common.base.Optional
import org.apache.hadoop.conf.Configuration
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark._
import org.apache.spark.AccumulatorParam._
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.api.java.JavaNewHadoopRDD
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.AssumedPartitionsJavaNewHadoopRDD
/**
 * @author igor
 */
class AssumedPartitionsJSC {
  
}

object AssumedPartitionsJSC {

  def createAssumedJavaPairRdd[K, V](jrdd: JavaPairRDD[K, V], partitioner: Partitioner, verify: Boolean = true): JavaPairRDD[K, V] = {
    implicit val ktag: ClassTag[K] = fakeClassTag
    implicit val vtag: ClassTag[V] = fakeClassTag
    val srdd = AssumedPartitionsPairRDDFunctions(jrdd.rdd)
    val rdd = srdd.assumePartitionedBy(partitioner, verify)
    JavaPairRDD.fromRDD(rdd);
  }
  
  /**
   * Get an RDD with assumed partitioner! for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def assumedPartitionsHadoopFile[K, V, F <: NewInputFormat[K, V]](
    ctx: JavaSparkContext, 
    path: String, 
    fClass: Class[F], 
    kClass: Class[K], 
    vClass: Class[V], 
    conf: Configuration): JavaPairRDD[K, V] = ctx.sc.withScope {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)

    // The call to new NewHadoopJob automatically adds security credentials to conf,
    // so we don't need to explicitly add them ourselves
    val job = new NewHadoopJob(conf)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updatedConf = job.getConfiguration
    val rdd = new AssumedPartitionsNewHadoopRDD(ctx.sc, fClass, kClass, vClass, updatedConf).setName(path)
    
    new AssumedPartitionsJavaNewHadoopRDD(rdd.asInstanceOf[AssumedPartitionsNewHadoopRDD[K, V]])
  }
}