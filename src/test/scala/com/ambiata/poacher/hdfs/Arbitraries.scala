package com.ambiata.poacher.hdfs

import org.scalacheck._
import Arbitrary._
import scalaz._, Scalaz._, effect.IO
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.disorder.GenPlus

object Arbitraries {
  implicit def HdfsTemporaryArbitrary: Arbitrary[HdfsTemporary] = Arbitrary(for {
    z <- arbitrary[SubPath]
    f <- Gen.oneOf("", "/")
  } yield HdfsTemporary(s"temporary-${java.util.UUID.randomUUID().toString}/" + z.path + f))

  case class SubPath(path: String)
  implicit def SubPathArbitrary: Arbitrary[SubPath] =
    Arbitrary(GenPlus.listOfSized(1, 5, Gen.identifier).map(_.mkString("/")).map(SubPath.apply))
}
