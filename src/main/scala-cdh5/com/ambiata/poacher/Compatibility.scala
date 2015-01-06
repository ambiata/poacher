package com.ambiata.poacher

import com.ambiata.poacher.mr.DistCache

import org.apache.hadoop.conf.Configuration

object Compatibility {
  def findCacheFile(conf: Configuration, nskey: DistCache.NamespacedKey): String =
    nskey.combined
}
