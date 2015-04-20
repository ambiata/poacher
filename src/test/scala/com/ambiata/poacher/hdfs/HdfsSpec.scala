package com.ambiata.poacher.hdfs

import Arbitraries._
import com.ambiata.disorder.Ident
import com.ambiata.mundane.io.FilePath
import org.scalacheck._, Arbitrary._, Gen._
import org.specs2._
import org.apache.hadoop.fs.Path
import java.io.File
import com.ambiata.mundane.testing.RIOMatcher._
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class HdfsSpec extends Specification with ScalaCheck { def is = s2"""

 The Hdfs object provide functions to deal with paths
   it is possible to recursively glob files                         $globFiles
   we can create a glob to get all the leaves files in a directory  $filesGlob
   read / write bytes                                               $readWriteBytes

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

  implicit val parameters = set(minTestsOk = 10)

  def globFiles =  prop((dir: HdfsTemporary, files: List[FilePath]) => (for {
    d <- dir.path
    _ <- files.traverseU(f => Hdfs.write(new Path(d, f.path), "c"))
    c <- Hdfs.globFilesRecursively(d)
  } yield c).run(new Configuration) must beOkLike(_.size ==== files.size))

  def filesGlob = prop((dir: HdfsTemporary, files: List[FilePath]) => (for {
    d     <- dir.path
    paths =  files.map(f => new Path(d, f.path))
    maxDepth = paths.map(_.depth).maximum.getOrElse(0)
    leaves = paths.filter(_.depth == maxDepth)
    _     <- paths.traverseU(p => Hdfs.write(p, "c"))
    glob  <- Hdfs.filesGlob(d)
    fs    <- Hdfs.globFilesRecursively(d, glob.getOrElse("*"))
  } yield (fs, leaves)).run(new Configuration) must beOkLike { case (fs, leaves) =>
    fs.map(_.getName).toSet ==== leaves.map(_.getName).toSet
  })

  def readWriteBytes = prop((path: HdfsTemporary, bytes: Array[Byte]) => (for {
    p   <- path.path
    _   <- Hdfs.writeBytes(p, bytes)
    bs  <- Hdfs.readBytes(p)
  } yield bs).run(new Configuration) must beOkValue(bytes))

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

  def moveFileParentExists = prop((source: HdfsTemporary, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.mkdir(d.getParent)
    c <- moveFile(s, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))

  def moveDirCheck = prop((source: HdfsTemporary, sub: SubPath, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    c <- moveDir(s, sub, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))

  def moveDirParentExists = prop((source: HdfsTemporary, sub: SubPath, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.mkdir(d.getParent)
    c <- moveDir(s, sub, d, contents)
  } yield c).run(new Configuration) must beOkValue(contents))

  def moveParentFile = prop((source: HdfsTemporary, dest: HdfsTemporary, contents: String) => (for {
    d <- dest.path
    s <- source.path
    _ <- Hdfs.write(d.getParent, contents)
    c <- moveFile(s, d, contents)
  } yield c).run(new Configuration) must beFail)

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

  implicit def FilePathsArbitrary: Arbitrary[FilePath] = Arbitrary {
    for {
      n  <- Gen.choose(1, 5)
      fs <- Gen.listOfN(n, arbitrary[Ident]).map(ls => FilePath.unsafe(ls.map(_.value).mkString("/")))
    } yield fs
  }
}
