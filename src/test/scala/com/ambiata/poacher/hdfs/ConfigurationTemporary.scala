package com.ambiata.poacher.hdfs

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.Scoobi._
import scalaz._, Scalaz._, effect.IO

case class ConfigurationTemporary(path: String) {
  def conf: RIO[Configuration] = for {
    d <- LocalTemporary(Temporary.uniqueLocalPath, path).directory // Hdfsize
    c = new Configuration
    _ = c.set("hadoop.tmp.dir", d.path)
    _ = c.set("scoobi.dir", d.path + "/")
  } yield c
}

object ConfigurationTemporary {
  def random: ConfigurationTemporary =
    ConfigurationTemporary(s"temporary-${java.util.UUID.randomUUID().toString}")

  /** Deprecated callbacks. Use `ConfigurationTemporary.conf` */
  def withConf[A](f: Configuration => RIO[A]): RIO[A] =
    runWithConf(s"temporary-${java.util.UUID.randomUUID().toString}", f)

  def runWithConf[A](dir: String, f: Configuration => RIO[A]): RIO[A] =
    ConfigurationTemporary(dir).conf >>= (c => f(c))

  def runWithScoobiConf[A](dir: String, f: ScoobiConfiguration => RIO[A]): RIO[A] =
    ConfigurationTemporary(dir).conf >>= (c => f(c))

  def withConfX[A](f: Configuration => A): RIO[A] =
    withConf(conf => RIO.ok[A](f(conf)))
}
