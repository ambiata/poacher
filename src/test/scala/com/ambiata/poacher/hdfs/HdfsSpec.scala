package com.ambiata.poacher.hdfs

import Arbitraries._
import org.specs2._
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.RIOMatcher._
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class HdfsSpec extends Specification with ScalaCheck { def is = s2"""

 The Hdfs object provide functions to deal with paths
   it is possible to recursively glob paths  $e1

 Hdfs Finalizers
 ===============

   clean up resources                  $cleanup
   handles failure                     $handlesFailure
   clean up resources with failure     $cleanupFailure

 Moving Stuff
 ============

   Can move a file                                               $moveFileCheck
   Can move a dir                                                $moveDirCheck
   Can move a file to a dir which already exists                 $moveFileParentExists
   Can move a dir to a dir which already exists                  $moveDirParentExists
   Move will fail if the destination parent dir is a file        $moveParentFile

"""

 val basedir = "target/test/HdfsSpec/" + java.util.UUID.randomUUID()

 def e1 = {
   val dirs = Seq(basedir + "/a/b/c", basedir + "/e/f/g")
   val files = dirs.flatMap(dir => Seq(dir+"/f1", dir+"/f2"))
   dirs.foreach(dir => new File(dir).mkdirs)
   files.foreach(f => new File(f).createNewFile)

   Hdfs.globFilesRecursively(new Path(basedir)).run(new Configuration) must beOkLike(paths => paths must haveSize(4))
 }

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

  def moveFileCheck = prop((source: HdfsTemporary, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    c <- moveFile(s, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))
  .set(minTestsOk = 10)

  def moveFileParentExists = prop((source: HdfsTemporary, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.mkdir(d.getParent)
    c <- moveFile(s, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))
  .set(minTestsOk = 10)

  def moveDirCheck = prop((source: HdfsTemporary, sub: SubPath, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    c <- moveDir(s, sub, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))
  .set(minTestsOk = 10)

  def moveDirParentExists = prop((source: HdfsTemporary, sub: SubPath, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.mkdir(d.getParent)
    c <- moveDir(s, sub, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))
  .set(minTestsOk = 10)

  def moveParentFile = prop((source: HdfsTemporary, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.write(d.getParent, contents)
    c <- moveFile(s, d, contents)
  } yield c).run(new Configuration) must beFail)
  .set(minTestsOk = 10)

  def moveFile(source: Path, dest: Path, contents: String): Hdfs[String] = for {
    _ <- Hdfs.write(source, contents)
    r <- Hdfs.mv(source, dest)
    c <- Hdfs.readContentAsString(dest)
  } yield c

  def moveDir(source: Path, sub: SubPath, dest: Path, contents: String): Hdfs[String] = for {
    _ <- Hdfs.mkdir(source)
    _ <- Hdfs.write(new Path(source, sub.path), contents)
    r <- Hdfs.mv(source, dest)
    c <- Hdfs.readContentAsString(new Path(dest, sub.path))
  } yield c
}
