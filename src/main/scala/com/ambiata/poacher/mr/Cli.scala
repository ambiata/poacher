package com.ambiata.poacher.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

object Cli {
  def parseHadoopArguments(args: Array[String]): (Configuration, Array[String]) = {
    val configuration = new Configuration
    val parser = new GenericOptionsParser(configuration, args)
    (configuration, parser.getRemainingArgs)
  }
}
