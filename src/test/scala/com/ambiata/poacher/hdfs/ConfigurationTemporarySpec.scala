package com.ambiata.poacher.hdfs

import com.ambiata.disorder._
import com.ambiata.poacher.hdfs.ConfigurationTemporary._
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._
import org.apache.hadoop.fs.Path
import java.io.File
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

class ConfigurationTemporarySpec extends Specification with ScalaCheck { def is = s2"""

 Configuration
 =============

  Scoobi configuration ends in `/`
   ${ withConfX(conf => conf.get("scoobi.dir")) must beOkLike(s => s.endsWith("/"))  }

   ${ prop((id: Ident) => ConfigurationTemporary(id.value).conf.map(_.get("scoobi.dir")) must
        beOkLike(_.endsWith(id.value + "/"))) }

   ${ prop((id: Ident) => ConfigurationTemporary(id.value + "/").conf.map(_.get("scoobi.dir")) must
        beOkLike(_.endsWith(id.value + "/"))) }


  Configuration has non-default hadoop.tmp.dir
   ${ withConfX(conf => conf.get("hadoop.tmp.dir")) must beOkLike(s => s /== (new Configuration()).get("hadoop.tmp.dir")) }

   ${ prop((id: Ident) => ConfigurationTemporary(id.value).conf.map(_.get("hadoop.tmp.dir")) must
        beOkLike(s => s /== (new Configuration()).get("hadoop.tmp.dir"))) }

  Cleans up it's own resources  $cleanup

"""

  def cleanup = prop((id: Ident, data: String) => for {
    c <- ConfigurationTemporary.random.conf
    z = c.get("hadoop.tmp.dir")
    p = new Path(z, id.value)
    _ <- Hdfs.write(p, data).run(c)
    b <- Hdfs.exists(p).run(c)
    _ <- RIO.unsafeFlushFinalizers
    a <- Hdfs.exists(p).run(c)
  } yield b -> a ==== true -> false)
}
