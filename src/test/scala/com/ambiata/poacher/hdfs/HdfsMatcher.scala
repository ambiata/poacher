package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs.ConfigurationTemporary._
import org.apache.hadoop.conf.Configuration
import org.specs2._, matcher._, execute.{Result => SpecsResult, Error => SpecsError, _}, execute.Result.ResultMonoid
import org.scalacheck.Prop

import scalaz.{Success => _, Failure => _, _}, Scalaz._, effect.IO

object HdfsMatcher extends ThrownExpectations with ScalaCheckMatchers {
  implicit def HdfsAsResult[A: AsResult]: AsResult[Hdfs[A]] = new AsResult[Hdfs[A]] {
    def asResult(t: => Hdfs[A]): SpecsResult =
      implicitly[AsResult[RIO[A]]].asResult(withConf(c => t.run(c)))
  }

  implicit def HdfsProp[A: AsResult](r: => Hdfs[A]): Prop =
    resultProp(AsResult(r))

}
