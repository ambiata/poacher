package com.ambiata.poacher.mr

import com.ambiata.poacher.Compatibility
import com.ambiata.poacher.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import java.net.URI

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This is module for managing passing data-types via the distributed cache. This is
 * _unsafe_ at best, and should be used with extreme caution. The only valid reason to
 * use it is when writing raw map reduce jobs.
 */
case class DistCache(base: HdfsPath, contextId: ContextId) {

  /** Push a representation of a data-type to the distributed cache for this job, under the
     specified key. A namespace is added to the key to make it unique for each instance
     of DistCache and is maintained through the configuration object. This fails _hard_ if
     anything goes wrong. Use DistCache#pop in the setup method of Mapper or Reducer to
     recover data. */
  def push(job: Job, key: DistCache.Key, bytes: Array[Byte]): Unit =
    pushView(job, key, bytes, bytes.length)

  def pushView(job: Job, key: DistCache.Key, bytes: Array[Byte], length: Int): Unit = {
    val nskey = key.namespaced(contextId.value)
    val path = base /- nskey.combined
    val uri = new URI(path.path.path + "#" + nskey.combined)
    (path.writeWith(out => Hdfs.safe(out.write(bytes, 0, length))) >> Hdfs.safe {
      addCacheFile(uri, job)
    }).run(job.getConfiguration).unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(s"Could not push $nskey to distributed cache: ${Result.asString(e)}")
    }
  }

  /** Pop a data-type from the distributed job using the specified function, it is
     assumed that this is only run by map or reduce tasks where to the cache for
     this job where a call to DistCache#push has prepared everything. This fails
     _hard_ if anything goes wrong. */
  def pop[A](conf: Configuration, key: DistCache.Key, f: Array[Byte] => String \/ A): A = {
    val nskey = key.namespaced(contextId.value)
    val p = LocalPath.fromString(Compatibility.findCacheFile(conf, nskey))
    (for {
      d <- p.readBytes
      z <- RIO.fromOption(d, s"$p was empty.")
      r <- RIO.safe(f(z))
    } yield r).unsafePerformIO match {
      case Ok(\/-(a)) =>
        a
      case Ok(-\/(s)) =>
        sys.error(s"Could not decode ${nskey} on pop from local path: ${s}")
      case Error(e) =>
        sys.error(s"Could not pop ${nskey} from local path: ${Result.asString(e)}")
    }
  }
  def addCacheFile(uri: URI, job: Job): Unit = {
    import com.nicta.scoobi.impl.util.Compatibility.cache
    cache.addCacheFile(uri, job.getConfiguration)
  }
}

object DistCache {
  case class Key(value: String) {
    def namespaced(ns: String): NamespacedKey =
      NamespacedKey(ns, value)
  }
  case class NamespacedKey(namespace: String, value: String) {
    def combined: String =
      s"${namespace}${value}"
  }

  object Keys {
    val namespace = "ambiata.dist-cache.namespace"
  }
}
