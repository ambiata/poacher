package com.ambiata.poacher.mr

import com.ambiata.mundane.io._
import com.ambiata.poacher.thrift.KeyValueTest

import org.specs2._, matcher._

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class ThriftCacheSpec extends Specification with ScalaCheck { def is = s2"""

ThriftCache
-----------

  Accessible from mapper                    $map

"""
  def map = {
    val conf = new Configuration
    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("thrift-spec", job)
    job.setJobName("ThriftCacheSpec")
    job.setMapperClass(classOf[ThriftCacheSpecMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[Text])
    LocalPath.fromString("target/test/in/ThriftCacheSpec.csv").write("fred,10\nbarney,1000\n").unsafePerformIO
    FileInputFormat.addInputPaths(job, "target/test/in/ThriftCacheSpec.csv")
    FileOutputFormat.setOutputPath(job, new Path("target/test/out/ThriftCacheSpec-" + java.util.UUID.randomUUID))
    val kv = new KeyValueTest("key", "value")
    ctx.thriftCache.push(job, ThriftCache.Key("test"), kv)
    job.waitForCompletion(true)
  }
}

class ThriftCacheSpecMapper extends Mapper[LongWritable, Text, LongWritable, Text] {
  val value = new KeyValueTest
  var ctx: MrContext = null

  override def setup(context: Mapper[LongWritable, Text, LongWritable, Text]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ThriftCache.Key("test"), value)
    if (value.value != "value")
      sys.error("Did not deserialize KeyValueTest from cache")
  }
}
