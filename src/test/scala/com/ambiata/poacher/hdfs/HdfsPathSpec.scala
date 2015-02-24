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



"""

  implicit val BooleanMonoid: Monoid[Boolean] =
    scalaz.std.anyVal.booleanInstance.conjunction
}
