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
import scalaz._, Scalaz._, effect.Effect._

class HdfsPathSpec extends Specification with ScalaCheck { def is = s2"""


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



"""

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
