package com.dbarrett;

import java.nio.ByteBuffer;

/**
 * Created by dbarrett on 11/16/16.
 */
public class ByteOutput {
    public byte[] bytes;
    public int length;
    public ByteBuffer buffer;
    public ByteOutput(byte[] bytes, int length)
    {
        this.bytes = bytes;
        this.length = length;
    }
    public ByteOutput(ByteBuffer buffer, int length)
    {
        this.buffer = buffer;
        this.length = length;
    }
}
