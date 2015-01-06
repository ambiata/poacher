package com.ambiata.poacher

import com.ambiata.poacher.mr.DistCache

import org.apache.hadoop.conf.Configuration

object Compatibility {
  def findCacheFile(conf: Configuration, nskey: DistCache.NamespacedKey): String =
    com.nicta.scoobi.impl.util.Compatibility.cache.getLocalCacheFiles(conf).toList.find(_.getName == nskey.combined)
      .getOrElse(sys.error(s"Could not find $nskey to pop from local path.")).toString
}
