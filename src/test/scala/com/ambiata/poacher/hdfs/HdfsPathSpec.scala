package com.ambiata.poacher.hdfs

import org.specs2.Specification

import com.ambiata.disorder._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.path._
import com.ambiata.mundane.path.Arbitraries._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.poacher.hdfs.HdfsMatcher._

import org.apache.hadoop.conf.Configuration

import scala.io.Codec

import org.specs2._
import org.specs2.matcher.DisjunctionMatchers
import scalaz._, Scalaz._, effect.Effect._

class HdfsPathSpec extends Specification with ScalaCheck with DisjunctionMatchers { def is = s2"""


 HdfsPath
 ========

  HdfsPath operations should have the symenatics as Path operations

    ${ prop((l: Path, p: Path) => (HdfsPath(l) / p).path ==== l / p) }

    ${ prop((l: Path, p: Path) => (HdfsPath(l).join(p)).path ==== l.join(p)) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l) | p).path ==== (l | p)) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l).extend(p)).path ==== l.extend(p)) }

    ${ prop((l: Path, p: S) => (HdfsPath(l) /- p.value).path ==== l /- p.value) }

    ${ prop((l: Path, p: Component) => (HdfsPath(l) | p).rebaseTo(HdfsPath(l)).map(_.path) ==== (l | p).rebaseTo(l)) }

    ${ prop((l: Path) => HdfsPath(l).dirname.path ==== l.dirname) }

    ${ prop((l: Path) => HdfsPath(l).basename ==== l.basename) }

  A list of HdfsPath can be ordered

    ${ List(HdfsPath.fromString("z"), HdfsPath.fromString("a")).sorted ====
         List(HdfsPath.fromString("a"), HdfsPath.fromString("z")) }

 HdfsPath IO
 ===========

  HdfsPath should be able to determine files, directories and handle failure cases

    ${ HdfsTemporary.random.path.flatMap(path => path.touch >> path.determine.map(_ must beFile)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.determine.map(_ must beDirectory)) }

    ${ HdfsTemporary.random.path.flatMap(path => path.determine.map(_ must beNone)) }

    ${ HdfsPath(Path("empty")).determine.map(_ must beNone) }

  HdfsPath can determine a file and handle failure cases

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         f <- p.write("")
         r <- p.determineFile
       } yield r ==== f)
     }

    ${ HdfsTemporary.random.path.flatMap(path => path.mkdirs >> path.determineFile) must beFail }

    ${ HdfsTemporary.random.path.flatMap(path => path.determineFile) must beFail }

  HdfsPath can determine a directory and handle failure cases

    ${ HdfsTemporary.random.path.flatMap(path => path.touch >> path.determineDirectory) must beFail }

    ${ prop((h: HdfsTemporary) => for {
         p <- h.path
         f <- p.mkdirs
         r <- p.determineDirectory
       } yield r ==== f)
     }

    ${ HdfsTemporary.random.path.flatMap(path => path.determineDirectory) must beFail }

  HdfsPath should be able to perform these basic operations

    Check if a path exists

      ${ prop((l: HdfsTemporary) => l.path.flatMap(p => p.touch >> p.exists.map(_ ==== true))) }

      ${ prop((l: HdfsTemporary) => l.path.flatMap(p => p.mkdirs >> p.exists.map(_ ==== true))) }

      ${ prop((l: HdfsTemporary) => l.path.flatMap(_.exists.map(_ ==== false))) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(p => p.touch >> p.doesExist("",
           Hdfs.io({ i = 1; i })).map(_ ==== 1)) }) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(p => p.touch >>
           p.whenExists(Hdfs.io(i = 1).map(_ => i ==== 1))) }) }

      ${ prop((l: HdfsTemporary) => { var i = 0; l.path.flatMap(_.doesNotExist("",
           Hdfs.io({ i = 1; i })).map(_ ==== 1)) }) }


"""
  val beFile = beSome(be_-\/[HdfsFile])
  val beDirectory = beSome(be_\/-[HdfsDirectory])

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
