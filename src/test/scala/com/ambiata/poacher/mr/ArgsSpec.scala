package com.ambiata.poacher.mr

import org.scalacheck._
import org.specs2._

class ArgsSpec extends Specification with ScalaCheck { def is = s2"""

  Parse args and create configuration           $configuration
"""

  def configuration = prop { (args: List[String], hadoopArgs: HadoopArgs) =>
    val (config, remainingArgs) = Args.configuration(hadoopArgs.args.map(_.arg) ++ args)
    val parsedValues = hadoopArgs.args.map(a => config.get(a.k))
    val expectedValues = hadoopArgs.args.map(_.v)
    (remainingArgs, parsedValues) ==== (args -> expectedValues)
  }

  case class HadoopArg(k: String, v: String) {
    def arg: String = "-D" + k + "=" + v
  }

  implicit def HadoopArgArbitrary: Arbitrary[HadoopArg] = Arbitrary(for {
    k <- Gen.identifier
    v <- Gen.identifier
  } yield HadoopArg(k, v))

  // Make sure the arg keys are unique
  case class HadoopArgs(args: List[HadoopArg])

  implicit def HadoopArgsArbitrary: Arbitrary[HadoopArgs] = Arbitrary(
    Arbitrary.arbitrary[List[HadoopArg]]
      .map(_.groupBy(_.k).mapValues(_.head).values.toList)
      .map(HadoopArgs)
  )
}
