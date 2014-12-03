package com.ambiata.poacher.hdfs

import com.ambiata.poacher.hdfs.TemporaryConfiguration._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.specs2._
import org.apache.hadoop.fs.Path
import java.io.File
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class TemporaryConfigurationSpec extends Specification with ScalaCheck { def is = s2"""

 Configuration
 =============

  Scoobi configuration ends in `/`
   ${ withConfX(conf => conf.get("scoobi.dir")) must beOkLike(s => s.endsWith("/"))  }

  Configuration has non-default hadoop.tmp.dir
   ${ withConfX(conf => conf.get("hadoop.tmp.dir")) must beOkLike(s => s /== (new Configuration()).get("hadoop.tmp.dir")) }

"""
}
