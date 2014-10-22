package com.ambiata.poacher

package object mr {
  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]
}
