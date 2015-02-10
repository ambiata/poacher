package com.ambiata.poacher.hdfs

import org.scalacheck._
import Arbitrary._
import scalaz._, Scalaz._, effect.IO
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

object Arbitraries {
  implicit def HdfsTemporaryArbitrary: Arbitrary[HdfsTemporary] = Arbitrary(for {
    i <- Gen.choose(1, 10)
    a <- Gen.listOfN(i, Gen.alphaNumChar)
    z = a.mkString("/")
    f <- Gen.oneOf("", "/")
  } yield HdfsTemporary(HdfsTemporary.hdfsTemporaryPath,
    s"${java.util.UUID.randomUUID().toString}/" + z + f))
}
