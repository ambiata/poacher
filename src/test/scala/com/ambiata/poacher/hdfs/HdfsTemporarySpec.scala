package com.ambiata.poacher.hdfs

import com.ambiata.disorder._
import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.poacher.hdfs.HdfsMatcher._
import org.apache.hadoop.fs.Path

import org.specs2._

import scalaz.{Store => _, _}, Scalaz._, effect.IO

class HdfsTemporarySpec extends Specification with ScalaCheck { def is = s2"""

 Temporary should clean up its own resources
 ===========================================

   clean up a file                                   $file
   clean up a directory                              $directory
   no conflicts                                      $conflicts

"""
  /** Testing Temporary clean up finalizers */
  def file = prop((data: String, tmp: HdfsTemporary) => for {
    f <- tmp.path
    _ <- Hdfs.write(f, data)
    e <- Hdfs.exists(f)
    _ <- Hdfs.fromRIO(RIO.unsafeFlushFinalizers)
    z <- Hdfs.exists(f)
  } yield e -> z ==== true -> false)

  /** Testing Temporary clean up finalizers */
  def directory = prop((data: String, id: Ident, hdfs: HdfsTemporary) => for {
    d <- hdfs.path
    f = new Path(d, id.value)
    _ <- Hdfs.write(f , data)
    e <- Hdfs.exists(f)
    _ <- Hdfs.fromRIO(RIO.unsafeFlushFinalizers)
    z <- Hdfs.exists(d)
  } yield e -> z ==== true -> false)

  def conflicts = prop((hdfs: HdfsTemporary, i: NaturalInt) => i.value > 0 ==> (for {
    l <- (1 to i.value % 100).toList.traverse(i => hdfs.path)
  } yield l.distinct ==== l))
}
