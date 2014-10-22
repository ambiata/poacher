package com.ambiata.poacher.mr

import org.scalacheck._, Arbitrary._

object Arbitraries {
  implicit def KeyValueArbitrary: Arbitrary[KeyValue] =
    Arbitrary(for {
      k <- arbitrary[String] suchThat (_.length > 0)
      v <- arbitrary[String] suchThat (_.length > 0)
      z <- arbitrary[Option[Int]]
    } yield KeyValue(k, v, z))
}
