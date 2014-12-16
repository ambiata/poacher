package com.ambiata.poacher.mr;

public class ByteView {

    public final byte[] bytes;
    public final int length;

    public ByteView(byte[] bytes, int length) {
        this.bytes = bytes;
        this.length = length;
    }
}
