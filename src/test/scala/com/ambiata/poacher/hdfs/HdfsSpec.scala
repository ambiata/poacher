package com.ambiata.poacher.hdfs

import org.specs2.Specification
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.RIOMatcher._
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class HdfsSpec extends Specification { def is = s2"""

 The Hdfs object provide functions to deal with paths
   it is possible to recursively glob paths  e1

 Hdfs Finalizers
 ===============

   clean up resources                  $cleanup
   handles failure                     $handlesFailure
   clean up resources with failure     $cleanupFailure

"""

 val basedir = "target/test/HdfsSpec/" + java.util.UUID.randomUUID()
/*
 def e1 = {
   val dirs = Seq(basedir + "/a/b/c", basedir + "/e/f/g")
   val files = dirs.flatMap(dir => Seq(dir+"/f1", dir+"/f2"))
   dirs.foreach(dir => new File(dir).mkdirs)
   files.foreach(f => new File(f).createNewFile)

   Hdfs.globFilesRecursively(new Path(basedir)).run(new Configuration) must beOkLike(paths => paths must haveSize(4))
 }
 */
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
