package com.ambiata.poacher.mr

import com.ambiata.poacher.thrift.KeyValueTest
import com.ambiata.poacher.mr.Arbitraries._
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import org.specs2.{ScalaCheck, Specification}

class ThriftDerserializationBehaviourSpec extends Specification with ScalaCheck { def is = s2"""

  Thrift Derserialization Behaviour
  ---------------------------------

  Clearing a thrift object is crucial!!!                            $clearUnsafe
  Clearing a thrift object is unnecessary for ThriftSerialiser      $clear
  Thrift facts with optional int is smaller than mandatory          $size
"""

  def clear = prop((f1: KeyValue, f2: KeyValue, f3: KeyValue, t: Int) => {
    val serialiser = ThriftSerialiser()
    f1.toThrift.unsetOption()
    // We want to show that clearing here (for our own ThriftSerialiser) is redundant (especially with optional fields)
    // In particular ThriftFact has optional int will sometimes not be set
    f2.toThrift.clear()
    f3.toThrift.setOption(t)
    val bytes = serialiser.toBytes(f1.toThrift)
    // We want to compare deserialisation to one "dirty" (_and_ different) thrift object to a cleared one
    serialiser.fromBytesUnsafe(f2.toThrift, bytes) ==== serialiser.fromBytesUnsafe(f3.toThrift, bytes)
  }).set(minTestsOk = 3)

  def clearUnsafe = prop((f1: KeyValue, f2: KeyValue, f3: KeyValue, t: Int) => {
    val serialiser = new TSerializer(new TCompactProtocol.Factory)
    val deserialiser = new TDeserializerCopy(new TCompactProtocol.Factory)
    f1.toThrift.unsetOption()
    f2.toThrift.clear()
    f3.toThrift.setOption(t)
    val bytes = serialiser.serialize(f1.toThrift)
    deserialiser.deserialize(f2.toThrift, bytes)
    deserialiser.deserialize(f3.toThrift, bytes)
    // Normally we would expect these to be the same, but we have set an optional field that was blank in f1
    f2.toThrift !=== f3.toThrift
  }).set(minTestsOk = 3)

  def size = prop((kv: KeyValue, option: Int) => {
    val serialiser = ThriftSerialiser()
    val f2 = new KeyValueTest(kv.key, kv.toThrift.getValue)
    serialiser.toBytes(kv.toThrift).length must lessThanOrEqualTo(serialiser.toBytes(f2).length)
  })
}
