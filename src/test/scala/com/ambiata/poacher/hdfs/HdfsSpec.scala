package com.ambiata.poacher.hdfs

import Arbitraries._
import org.specs2._
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.RIOMatcher._
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class HdfsSpec extends Specification with ScalaCheck { def is = s2"""

 Hdfs Finalizers
 ===============

   clean up resources                  $cleanup
   handles failure                     $handlesFailure
   clean up resources with failure     $cleanupFailure

"""

  def cleanup = {
    var v = 0
    Hdfs.addFinalizer(Hdfs.safe(v = 1)).run(new Configuration).unsafePerformIO
    v ==== 1
  }

  def handlesFailure = {
    var v = 0
    Hdfs.addFinalizer(Hdfs.fail("")).run(new Configuration).unsafePerformIO
    v ==== 0
  }

  def cleanupFailure = {
    var v = 0
    (Hdfs.addFinalizer(Hdfs.safe(v = 1)) >> Hdfs.fail("")).run(new Configuration).unsafePerformIO
    v ==== 1
  }
}
