package com.dbarrett;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by dbarrett on 11/24/16.
 */
public class MappedBlockByteParser implements Callable<ByteOutput> {
    private static byte delim = 0x01;
    private static byte equals = 0x3D;
    private static byte newLine = 0x0A;
    private static byte period = 0x2E;
    private static byte zero = 0x30;
    private static byte one = 0x31;
    private static byte four = 0x34;
    private static byte five = 0x35;
    private static byte seven = 0x37;
    private static byte eight = 0x38;
    private static byte nine = 0x39;
    private static byte semi = 0x3A;
    private static byte minus = 0x2D;
    private static byte[] lastUpdateB = {0x09, 0x4C, 0x61, 0x73, 0x74, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6D, 0x65, 0x3D,};
    private static byte[] lowLimitB = {0x09, 0x4C, 0x6F, 0x77, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] highLimitB = {0x09, 0x48, 0x69, 0x67, 0x68, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] limitPriceB = {0x09, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x52, 0x61, 0x6E, 0x67, 0x65, 0x3D};
    private static byte[] tradingB = {0x09, 0x54, 0x72, 0x61, 0x64, 0x69, 0x6E, 0x67, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] nullB = {0x4E, 0x55, 0x4C, 0x4C};
    private Calendar cal = new GregorianCalendar();
    private ByteBuffer bytes, out;
    private int nRead, offset;
    private static HashMap<Long, Long> calendarShort = new HashMap<Long, Long>();
    KeyValueOffsets tag48 = new KeyValueOffsets();
    KeyValueOffsets tag55 = new KeyValueOffsets();
    KeyValueOffsets tag779 = new KeyValueOffsets();
    KeyValueOffsets tag1148 = new KeyValueOffsets();
    KeyValueOffsets tag1149 = new KeyValueOffsets();
    KeyValueOffsets tag1150 = new KeyValueOffsets();

    public MappedBlockByteParser(ByteBuffer buffer, int offset, int nRead) {
        this.bytes = buffer;
        this.nRead = nRead;
        this.offset = offset;
        this.out = ByteBuffer.allocate(nRead - offset);
    }

    @Override
    public ByteOutput call() throws Exception {
        parseLineNoCopy();
        this.out.limit(this.out.position());
        this.out.flip();
        return new ByteOutput(this.out, this.out.position());
    }

    private void parseLineNoCopy() throws Exception {
        for (int byteOffset = offset; byteOffset < nRead; ) {
            for (; byteOffset < nRead; byteOffset++) {
                if (byteOffset + 4 >= nRead) {
                    byteOffset = nRead;
                    continue;
                }
                if(this.bytes.get(byteOffset) == newLine)
                {
                    byteOffset++;
                    break;
                }
                int plus1 = byteOffset + 1;
                int plus2 = byteOffset + 2;
                int plus3 = byteOffset + 3;
                int plus4 = byteOffset + 4;
                //go through the buffer and find the field's in the line
                byte f = this.bytes.get(byteOffset);
                if (f == four && this.bytes.get(plus1) == eight && this.bytes.get(plus2) == equals) {
                    tag48.setValueStart(plus3);
                    byteOffset = plus3;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag48.setValueEnd(byteOffset);
                } else if (f == five && this.bytes.get(plus1) == five && this.bytes.get(plus2) == equals) {
                    tag55.setValueStart(plus3);
                    byteOffset = plus3;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag55.setValueEnd(byteOffset);
                } else if (f == seven && this.bytes.get(plus1) == seven && this.bytes.get(plus2) == nine && this.bytes.get(plus3) == equals) {
                    tag779.setValueStart(plus4);
                    byteOffset = plus4;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag779.setValueEnd(byteOffset);
                } else if (f == one && this.bytes.get(plus1) == one) {
                    int plus5 = plus4+1;
                    if (this.bytes.get(plus2) == four && this.bytes.get(plus3) == eight && this.bytes.get(plus4) == equals) {
                        tag1148.setValueStart(plus5);
                        byteOffset = plus5;
                        //move to the end of the field
                        while (this.bytes.get(byteOffset) != delim)
                            byteOffset++;
                        tag1148.setValueEnd(byteOffset);
                    } else if (this.bytes.get(plus2) == four && this.bytes.get(plus3) == nine && this.bytes.get(plus4) == equals) {
                        tag1149.setValueStart(plus5);
                        byteOffset = plus5;
                        //move to the end of the field
                        while (this.bytes.get(byteOffset) != delim)
                            byteOffset++;
                        tag1149.setValueEnd(byteOffset);
                    } else if (this.bytes.get(plus2) == five && this.bytes.get(plus3) == zero && this.bytes.get(plus4) == equals) {
                        tag1150.setValueStart(plus5);
                        byteOffset = plus5;
                        //move to the end of the field
                        while (this.bytes.get(byteOffset) != delim)
                            byteOffset++;
                        tag1150.setValueEnd(byteOffset);
                    } else {
                        //go to next field
                        do {
                            ++byteOffset;
                        } while (this.bytes.get(byteOffset) != delim);
                    }
                } else {
                    //go to next field
                    do {
                        ++byteOffset;
                    } while (this.bytes.get(byteOffset) != delim);
                }
            }
            //construct the message tag
            copyToFinal(tag48.isFound(), tag48.getValueStart(), tag48.getValueEnd());
            out.put(MappedBlockByteParser.semi);
            copyToFinal(tag55.isFound(), tag55.getValueStart(), tag55.getValueEnd());
            out.put(MappedBlockByteParser.newLine);
            //construct the date message

            out.put(MappedBlockByteParser.lastUpdateB);
            ToDateEpochString(tag779);
            out.put(MappedBlockByteParser.newLine);

            //construct low limit message
            out.put(MappedBlockByteParser.lowLimitB);
            copyToFinal(tag1148.isFound(), tag1148.getValueStartDontReset(), tag1148.getValueEnd());
            out.put(MappedBlockByteParser.newLine);

            //construct high limit message
            out.put(MappedBlockByteParser.highLimitB);
            copyToFinal(tag1149.isFound(), tag1149.getValueStartDontReset(), tag1149.getValueEnd());
            out.put(MappedBlockByteParser.newLine);

            out.put(MappedBlockByteParser.limitPriceB);
            ComputeLimitRangeString(tag1148, tag1149);
            out.put(MappedBlockByteParser.newLine);

            out.put(MappedBlockByteParser.tradingB);
            copyToFinal(tag1150.isFound(), tag1150.getValueStart(), tag1150.getValueEnd());
            out.put(MappedBlockByteParser.newLine);
        }
    }

    private void copyToFinal(boolean found, int valueStart, int valueEnd) {
        if (found) {
            for (; valueStart < valueEnd; valueStart++)
                out.put(this.bytes.get(valueStart));
        } else {
            out.put(MappedBlockByteParser.nullB);
        }
    }

    private void ToDateEpochString(KeyValueOffsets dateO) {
        long dateFromEpoch = 0;
        try {
            Long hash = this.bytes.getLong(dateO.getValueStart());
            int valueStart = dateO.getValueStart();
            Long millis = MappedBlockByteParser.calendarShort.get(hash);
            if (millis == null) {
                int year = parseInt(valueStart, valueStart + 4);
                int month = parseInt(valueStart + 4, valueStart + 6);
                int day = parseInt(valueStart + 6, valueStart + 8);
                cal.set(year, month, day, 0, 0, 0);
                millis = cal.getTimeInMillis();
                MappedBlockByteParser.calendarShort.put(hash, millis);
            }

            int hour = parseInt(valueStart + 8, valueStart + 10) * 3600000;
            int minute = parseInt(valueStart + 10, valueStart + 12) * 60000;
            int second = parseInt(valueStart + 12, valueStart + 14) * 1000;
            dateFromEpoch = (millis.longValue() + hour + minute + second) * 1000;
        } catch (Exception e) {
            e.printStackTrace();
        }
        //could pull the microseconds from the field
        long power = 1000000000000000L;
        for (int i = 0; i < 16; i++) {
            out.put((byte) ((dateFromEpoch / power) + 48));
            dateFromEpoch %= power;
            power /= 10;
        }
    }

    private void ComputeLimitRangeString(KeyValueOffsets lowPrice, KeyValueOffsets highPrice) throws Exception {
        try {
            if (!lowPrice.isFound() || !highPrice.isFound()) {
                out.put(MappedBlockByteParser.nullB);
            } else {
                //copy the high price into a temp buffer to do the subtraction in
                int highValueStart = highPrice.getValueStart();
                int highValueEnd = highPrice.getValueEnd();
                int lowValueStart = lowPrice.getValueStart();
                int lowValueEnd = lowPrice.getValueEnd();
                byte[] highPriceB = new byte[highValueEnd - highValueStart];
                for (int i = 0; i + highValueStart < highValueEnd; i++)
                    highPriceB[i] = this.bytes.get(i + highValueStart);

                //determine if the low price is negative or not
                //if it is negative just add the two long values
                int highIdx = highPriceB.length - 1;
                if (this.bytes.get(lowValueStart) == MappedBlockByteParser.minus) {
                    //low value is negative so add both fields
                    for (int i = lowValueEnd - 1; i > lowValueStart; i--) {
                        //handle when the negative number is larger than the high price
                        if (highIdx < 0) {
                            highPriceB = addByteAtBeginning(highPriceB);
                            highIdx++;
                        }

                        if (highPriceB[highIdx] != MappedBlockByteParser.period) {
                            highPriceB[highIdx] = (byte) (highPriceB[highIdx] + this.bytes.get(i) - MappedBlockByteParser.zero);
                            if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] > MappedBlockByteParser.nine) {
                                if (highIdx == 0) {
                                    highPriceB = addByteAtBeginning(highPriceB);
                                    highIdx++;
                                }
                                addTen(highPriceB, highIdx);
                            }
                        }
                        highIdx--;
                    }
                    while (highIdx >= 0) {
                        if (highPriceB[highIdx] != MappedBlockByteParser.period && highPriceB[highIdx] != 0x00 && highPriceB[highIdx] > MappedBlockByteParser.nine) {
                            if (highIdx == 0) {
                                highPriceB = addByteAtBeginning(highPriceB);
                                highIdx++;
                            }
                            addTen(highPriceB, highIdx);
                        }
                        highIdx--;
                    }

                } else {
                    //low value is positive so subtract both fields
                    for (int i = lowValueEnd - 1; i >= lowValueStart; i--) {
                        if (highPriceB[highIdx] != MappedBlockByteParser.period) {
                            if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] < MappedBlockByteParser.zero) {
                                borrowTen(highPriceB, highIdx);
                            }
                            highPriceB[highIdx] = (byte) ((highPriceB[highIdx] - this.bytes.get(i)) + MappedBlockByteParser.zero);
                            if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] < MappedBlockByteParser.zero) {
                                borrowTen(highPriceB, highIdx);
                            }
                        }
                        highIdx--;
                    }
                }
                out.put(highPriceB);
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private byte[] addByteAtBeginning(byte[] bytes) {
        byte[] temp = new byte[bytes.length + 1];
        temp[0] = MappedBlockByteParser.zero;
        for (int k = 1; k < bytes.length + 1; k++)
            temp[k] = bytes[k - 1];
        return temp;
    }

    private void addTen(byte[] bytes, int idx) {
        if (bytes[idx - 1] != MappedBlockByteParser.period)
            bytes[idx - 1] = (byte) (bytes[idx - 1] + 1);
        else
            bytes[idx - 2] = (byte) (bytes[idx - 2] + 1);
        bytes[idx] = (byte) (bytes[idx] - 10);
    }

    private void borrowTen(byte[] bytes, int idx) {
        if (bytes[idx - 1] != MappedBlockByteParser.period)
            bytes[idx - 1] = (byte) (bytes[idx - 1] - 1);
        else
            bytes[idx - 2] = (byte) (bytes[idx - 2] - 1);
        bytes[idx] = (byte) (bytes[idx] + 10);
    }


    private int parseInt(int start, int end) {
        int ret = 0;
        int power = 1;
        for (int i = end - 1; i >= start; i--) {
            ret += ((this.bytes.get(i) - zero) * power);
            power *= 10;
        }
        return ret;
    }
}
