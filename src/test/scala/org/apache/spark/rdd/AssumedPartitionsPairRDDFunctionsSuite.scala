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

import java.io.File
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.util.Progressable
import scala.collection.mutable.{ ArrayBuffer, HashSet }
import scala.util.Random
import org.apache.hadoop.conf.{ Configurable, Configuration }
import org.apache.spark.{ SparkException, HashPartitioner, Partitioner }
import org.apache.spark.util.Utils
import org.scalatest.FunSuite
import org.apache.spark.rdd.PairRDDFunctions._
import org.apache.spark.rdd.AssumedPartitionsPairRDDFunctions._
import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaSparkContext
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import com.holdenkarau.spark.testing.SharedSparkContext


class AssumedPartitionsSuite extends FunSuite with SharedSparkContext {
  
  test("assumePartitioned -- verify works") {
    val rdd = sc.parallelize(1 to 100, 10).groupBy(identity)
    //catch wrong number of partitions immediately
    val exc1 = intercept[SparkException] { rdd.assumePartitionedBy(new HashPartitioner(5)) }
    assert(exc1.getMessage() == ("Assumed Partitioner org.apache.spark.HashPartitioner@5 expects" +
      " 5 partitions, but there are 10 partitions.  If you are assuming a partitioner on a" +
      " HadoopRDD, you might need to disable input splits with a custom input format"))

    //also catch wrong partitioner (w/ right number of partitions) during action
    val assumedPartitioned = rdd.assumePartitionedBy(new Partitioner {
      override def numPartitions: Int = 10
      override def getPartition(key: Any): Int = 3
    })
    val exc2 = intercept[SparkException] { assumedPartitioned.collect() }
    assert(exc2.getMessage().contains(" was not in the assumed partition.  If you are assuming a" +
      " partitioner on a HadoopRDD, you might need to disable input splits with a custom input" +
      " format"))
  }
  
  test("assumePartitioned") {
    val nGroups = 20
    val nParts = 10
    val rdd: RDD[(Int, Int)] = sc.parallelize(1 to 100, nParts).map { x => (x % nGroups) -> x }.partitionBy(new HashPartitioner(nParts))
    val tempDir = Utils.createTempDir()
    val f = new File(tempDir, "assumedPartitionedSeqFile")
    val path = f.getAbsolutePath
    rdd.saveAsSequenceFile(path)
    val ctx = new JavaSparkContext(sc)

    // this is basically sc.sequenceFile[Int,Int], but with input splits turned off
    val reloaded: RDD[(Int, Int)] = AssumedPartitionsJSC.assumedPartitionsHadoopFile(
      ctx,
      path,
      classOf[NoSplitSequenceFileInputFormat[IntWritable, IntWritable]],
      classOf[IntWritable],
      classOf[IntWritable],
      sc.hadoopConfiguration).rdd.map { case (k, v) => k.get() -> v.get() }
      
    

    val assumedPartitioned = reloaded.assumePartitionedBy(rdd.partitioner.get)
    assumedPartitioned.count() //need an action to run the verify step

    val j1: RDD[(Int, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(assumedPartitioned)
    assert(j1.getNarrowAncestors.contains(rdd))
    assert(j1.getNarrowAncestors.contains(assumedPartitioned))

    j1.foreach {
      case (group, (left, right)) =>
        //check that we've got the same groups in both RDDs
        val leftSet = left.toSet
        val rightSet = right.toSet
        if (leftSet != rightSet) throw new RuntimeException("left not equal to right")
        //and check that the groups are correct
        leftSet.foreach { x => if (x % nGroups != group) throw new RuntimeException(s"bad grouping") }
    }

    // this is just to make sure the test is actually useful, and would catch a mistake if it was
    // *not* a narrow dependency
    val j2 = rdd.cogroup(reloaded)
    assert(!j2.getNarrowAncestors.contains(reloaded))
  }
    
}

class NoSplitSequenceFileInputFormat[K, V] extends SequenceFileInputFormat[K, V] {  
  override def isSplitable(jc: org.apache.hadoop.mapreduce.JobContext, file: Path) = false
}
