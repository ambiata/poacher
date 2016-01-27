package com.ambiata.poacher.hdfs

sealed trait HdfsWriteMode {
  def fold[X](
    overwrite: => X
  , fail: => X
  ): X = this match {
    case HdfsWriteMode.Overwrite =>
      overwrite
    case HdfsWriteMode.Fail =>
      fail
  }
}

object HdfsWriteMode {
  case object Overwrite extends HdfsWriteMode
  case object Fail extends HdfsWriteMode
}
