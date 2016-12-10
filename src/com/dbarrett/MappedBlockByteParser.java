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
    private static byte nullChar = 0x00;
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
    private KeyValueOffsets tag48 = new KeyValueOffsets();
    private KeyValueOffsets tag55 = new KeyValueOffsets();
    private KeyValueOffsets tag779 = new KeyValueOffsets();
    private KeyValueOffsets tag1148 = new KeyValueOffsets();
    private KeyValueOffsets tag1149 = new KeyValueOffsets();
    private KeyValueOffsets tag1150 = new KeyValueOffsets();
    private byte[] highPriceB = new byte[20];

    public MappedBlockByteParser(ByteBuffer buffer, int offset, int nRead) {
        this.bytes = buffer;
        this.nRead = nRead;
        this.offset = offset;
        this.out = ByteBuffer.allocate(nRead - offset);
    }

    @Override
    public ByteOutput call() throws Exception {
        parse();
        this.out.limit(this.out.position());
        this.out.flip();
        return new ByteOutput(this.out, this.out.position());
    }

    private void parse() throws Exception {
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
                byte firstByte = this.bytes.get(byteOffset);
                byte secondByte = this.bytes.get(plus1);
                byte thirdByte = this.bytes.get(plus2);
                byte fourthByte = this.bytes.get(plus3);
                byte fifthByte = this.bytes.get(plus4);
                if (firstByte == four && secondByte == eight && thirdByte == equals) {
                    tag48.setValueStart(plus3);
                    byteOffset = plus3;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag48.setValueEnd(byteOffset);
                } else if (firstByte == five && secondByte == five && thirdByte == equals) {
                    tag55.setValueStart(plus3);
                    byteOffset = plus3;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag55.setValueEnd(byteOffset);
                } else if (firstByte == seven && secondByte == seven && thirdByte == nine && fourthByte == equals) {
                    tag779.setValueStart(plus4);
                    byteOffset = plus4;
                    //move to the end of the field
                    while (this.bytes.get(byteOffset) != delim)
                        byteOffset++;
                    tag779.setValueEnd(byteOffset);
                } else if (firstByte == one && secondByte == one) {
                    int plus5 = plus4+1;
                    if (thirdByte == four && fourthByte == eight && fifthByte == equals) {
                        tag1148.setValueStart(plus5);
                        byteOffset = plus5;
                        //move to the end of the field
                        while (this.bytes.get(byteOffset) != delim)
                            byteOffset++;
                        tag1148.setValueEnd(byteOffset);
                    } else if (thirdByte == four && fourthByte == nine && fifthByte == equals) {
                        tag1149.setValueStart(plus5);
                        byteOffset = plus5;
                        //move to the end of the field
                        while (this.bytes.get(byteOffset) != delim)
                            byteOffset++;
                        tag1149.setValueEnd(byteOffset);
                    } else if (thirdByte == five && fourthByte == zero && fifthByte == equals) {
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
            boolean found1148 = tag1148.isFound();
            copyToFinal(found1148, tag1148.getValueStart(), tag1148.getValueEnd());
            out.put(MappedBlockByteParser.newLine);

            //construct high limit message
            out.put(MappedBlockByteParser.highLimitB);
            boolean found1149 = tag1149.isFound();
            copyToFinal(found1149, tag1149.getValueStart(), tag1149.getValueEnd());
            out.put(MappedBlockByteParser.newLine);

            out.put(MappedBlockByteParser.limitPriceB);
            ComputeLimitRangeString(tag1148, found1148, tag1149, found1149);
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

    private void ComputeLimitRangeString(KeyValueOffsets lowPrice, boolean foundLow, KeyValueOffsets highPrice, boolean foundHigh) throws Exception {
        try {
            if (foundLow && foundHigh) {
                //copy the high price into a temp buffer to do the subtraction in
                int highValueStart = highPrice.getValueStart();
                int highValueEnd = highPrice.getValueEnd();
                int lowValueStart = lowPrice.getValueStart();
                int lowValueEnd = lowPrice.getValueEnd();
                for(int i =0; i < 20; i++)
                    this.highPriceB[i] = MappedBlockByteParser.nullChar;

                //determine if the low price is negative or not
                //if it is negative add the two long values
                int resultIdx = highPriceB.length-1;
                int highIdx = highValueEnd-1;
                if (this.bytes.get(lowValueStart) == MappedBlockByteParser.minus) {
                    //low value is negative so add both fields
                    for (int i = lowValueEnd - 1; i > lowValueStart; i--) {
                        byte nextByte = this.bytes.get(highIdx);
                        //high value is shorter so don't pull any more values for it
                        if(highIdx < highValueStart)
                            nextByte = MappedBlockByteParser.zero;
                        if (nextByte != MappedBlockByteParser.period) {
                            highPriceB[resultIdx] = (byte)(highPriceB[resultIdx] + nextByte + this.bytes.get(i) - MappedBlockByteParser.zero);
                            if (highPriceB[resultIdx] > MappedBlockByteParser.nine)
                                addTen(highPriceB, resultIdx);
                        }
                        highIdx--;
                        if(nextByte != MappedBlockByteParser.period)
                            resultIdx--;
                    }
                    while (highIdx >= highValueStart) {
                        byte nextByte = this.bytes.get(highIdx);
                        //if(highPriceB[resultIdx] > MappedBlockByteParser.zero)
                            highPriceB[resultIdx] = (byte)(highPriceB[resultIdx]+nextByte);
                        //else
                        //    highPriceB[resultIdx] = nextByte;
                        if (highPriceB[resultIdx] > MappedBlockByteParser.nine)
                            addTen(highPriceB, resultIdx);
                        highIdx--;
                        resultIdx--;
                    }
                    if(highPriceB[resultIdx] > MappedBlockByteParser.nullChar) {
                        if(highPriceB[resultIdx] == 1)
                            highPriceB[resultIdx] = MappedBlockByteParser.one;
                        resultIdx--;
                    }
                } else {
                    //low value is positive so subtract both fields
                    for (int i = lowValueEnd - 1; i >= lowValueStart; i--) {
                        byte nextByte = this.bytes.get(highIdx);
                        if (nextByte != MappedBlockByteParser.period) {
                            //if (highPriceB[resultIdx] != MappedBlockByteParser.nullChar && highPriceB[resultIdx] < MappedBlockByteParser.zero) {
                            //    borrowTen(highPriceB, resultIdx);
                            //}
                            if(highPriceB[resultIdx] > MappedBlockByteParser.nullChar)
                                highPriceB [resultIdx] = (byte)(highPriceB[resultIdx] + nextByte - this.bytes.get(i));
                            else
                                highPriceB[resultIdx] = (byte) ((nextByte - this.bytes.get(i)) + MappedBlockByteParser.zero);
                            if (highPriceB[resultIdx] < MappedBlockByteParser.zero) {
                                borrowTen(highPriceB, resultIdx);
                            }
                        }
                        highIdx--;
                        if(nextByte != MappedBlockByteParser.period)
                            resultIdx--;
                    }
                    while (highIdx >= highValueStart) {
                        byte nextByte = this.bytes.get(highIdx);
                        if(highPriceB[resultIdx] > MappedBlockByteParser.nullChar)
                            highPriceB[resultIdx] = (byte)(highPriceB[resultIdx] + nextByte-MappedBlockByteParser.zero);
                        else
                            highPriceB[resultIdx] = nextByte;
                        if (highPriceB[resultIdx] < MappedBlockByteParser.zero) {
                            borrowTen(highPriceB, resultIdx);
                        }
                        resultIdx--;
                        highIdx--;
                    }
                }
                for (int i = resultIdx+1; i < highPriceB.length; i++) {
                    if (i == (highPriceB.length - 7))
                        out.put(MappedBlockByteParser.period);
                    out.put(highPriceB[i]);
                }

            } else {
                out.put(MappedBlockByteParser.nullB);
            }
            }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void addTen(byte[] bytes, int idx) {
        if (bytes[idx - 1] != MappedBlockByteParser.period)
            bytes[idx - 1] = (byte) (bytes[idx - 1] + 1);
        else
            bytes[idx - 2] = (byte) (bytes[idx - 2] + 1);
        bytes[idx] = (byte) (bytes[idx] - 10);
    }

    private void borrowTen(byte[] bytes, int idx) {
        if(bytes[idx - 1] == MappedBlockByteParser.nullChar)
            bytes[idx - 1] = (byte)(MappedBlockByteParser.zero-1);
        else if (bytes[idx - 1] != MappedBlockByteParser.period)
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
