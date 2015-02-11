package com.ambiata.poacher.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

object Args {

  def configuration(args: List[String]): (Configuration, List[String]) = {
    val parser = new GenericOptionsParser(args.toArray)
    (parser.getConfiguration, parser.getRemainingArgs.toList)
  }
}
