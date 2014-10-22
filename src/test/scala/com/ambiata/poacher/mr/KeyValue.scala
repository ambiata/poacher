package com.ambiata.poacher.mr

import com.ambiata.poacher.thrift.KeyValueTest

case class KeyValue(key: String, value: String, optional: Option[Int]) {
  def toThrift: KeyValueTest =
    new KeyValueTest(key, value)

  def getOptional: Int =
    Option(toThrift.getOption).getOrElse(0)

}
