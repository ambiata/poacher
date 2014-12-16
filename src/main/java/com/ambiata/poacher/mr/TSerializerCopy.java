package com.ambiata.poacher.mr;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * Generic utility for easily serializing objects into a byte array or Java
 * String.
 *
 * Copied from <a href="https://thrift.apache.org/">Thrift</a> so we can
 * access a private field.
 */
public class TSerializerCopy {

    /**
     * This is the byte array that data is actually serialized into
     */
    private final ByteArrayOutputStream baos_ = new ByteArrayOutputStream();

    /**
     * This transport wraps that byte array
     */
    private final TIOStreamTransport transport_ = new TIOStreamTransport(baos_);

    /**
     * Internal protocol used for serializing objects.
     */
    private TProtocol protocol_;

    /**
     * Create a new TSerializer that uses the TBinaryProtocol by default.
     */
    public TSerializerCopy() {
        this(new TBinaryProtocol.Factory());
    }

    /**
     * Create a new TSerializer. It will use the TProtocol specified by the
     * factory that is passed in.
     *
     * @param protocolFactory Factory to create a protocol
     */
    public TSerializerCopy(TProtocolFactory protocolFactory) {
        protocol_ = protocolFactory.getProtocol(transport_);
    }

    /**
     * Serialize the Thrift object into a byte array. The process is simple,
     * just clear the byte array output, write the object into it, and grab the
     * raw bytes.
     *
     * @param base The object to serialize
     * @return Serialized object in byte[] format
     */
    public byte[] serialize(TBase base) throws TException {
        baos_.reset();
        base.write(protocol_);
        return baos_.toByteArray();
    }

    /**
     * Serialize the Thrift object into a Java string, using a specified
     * character set for encoding.
     *
     * @param base The object to serialize
     * @param charset Valid JVM charset
     * @return Serialized object as a String
     */
    public String toString(TBase base, String charset) throws TException {
        try {
            return new String(serialize(base), charset);
        } catch (UnsupportedEncodingException uex) {
            throw new TException("JVM DOES NOT SUPPORT ENCODING: " + charset);
        }
    }

    /**
     * Serialize the Thrift object into a Java string, using the default JVM
     * charset encoding.
     *
     * @param base The object to serialize
     * @return Serialized object as a String
     */
    public String toString(TBase base) throws TException {
        return new String(serialize(base));
    }
}
