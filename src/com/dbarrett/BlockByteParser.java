package com.dbarrett;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by dbarrett on 11/14/16.
 */
public class BlockByteParser implements Callable<ByteOutput> {
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
    private static byte[] lowLimitB = { 0x09, 0x4C, 0x6F, 0x77, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] highLimitB = { 0x09, 0x48, 0x69, 0x67, 0x68, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] limitPriceB = {0x09, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x52, 0x61, 0x6E, 0x67, 0x65, 0x3D};
    private static byte[] tradingB = {0x09, 0x54,  0x72, 0x61, 0x64, 0x69, 0x6E, 0x67, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    private static byte[] nullB = {0x4E,0x55,0x4C,0x4C};
    private Calendar cal = new GregorianCalendar();
    private byte[] bytes;
    private byte[] outputBytes;
    private int outputIdx = 0;
    private int nRead;

    public BlockByteParser(byte[] buffer, int nRead)
    {
        this.bytes = buffer;
        this.nRead = nRead;
        this.outputBytes = new byte[buffer.length];
    }

    @Override
    public ByteOutput call() throws Exception {
        int j = 0;
        ArrayList<KeyValueOffsets> fields = new ArrayList<KeyValueOffsets>();
        for(int i = 0; j < nRead; i = j) {
            fields.clear();
            j = getFields(bytes, i, nRead, fields);
            if(j == i)
            {
                break;
            }
            parseLineNoCopy(bytes, fields);
        }
         return new ByteOutput(this.outputBytes, this.outputIdx);
    }
    private void parseLineNoCopy(byte[] bytes, List<KeyValueOffsets> keys) throws Exception
    {
        KeyValueOffsets tag48 = null;
        KeyValueOffsets tag55 = null;
        KeyValueOffsets tag779 = null;
        KeyValueOffsets tag1148 = null;
        KeyValueOffsets tag1149 = null;
        KeyValueOffsets tag1150 = null;
        //go through the buffer and find the field's in the line
        for (KeyValueOffsets field : keys) {
            int tagLength = field.getTagLength();
            byte f = bytes[field.keyStart];
            if (tagLength <= 3) {
                if (tagLength == 2) {
                    if (f == four && bytes[field.keyStart+1] == eight)
                        tag48 = field;
                    else if (f == five && bytes[field.keyStart+1] == five)
                        tag55 = field;
                } else if (tagLength == 3 && f == seven && bytes[field.keyStart+1] == seven && bytes[field.keyStart+2] == nine)
                    tag779 = field;
            } else if (tagLength == 4) {
                if (f == one && bytes[field.keyStart+1] == one) {
                    if (bytes[field.keyStart+2] == four) {
                        if (bytes[field.keyStart+3] == eight)
                            tag1148 = field;
                        else if (bytes[field.keyStart+3] == nine)
                            tag1149 = field;
                    }
                    else if (bytes[field.keyStart+2] == five && bytes[field.keyStart+3] == zero)
                        tag1150 = field;
                }
            }
        }
        //construct the message tag
        copyToFinal(tag48);
        this.outputBytes[outputIdx++] = BlockByteParser.semi;
        copyToFinal(tag55);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;
        //construct the date message

        copyToFinal(BlockByteParser.lastUpdateB);
        ToDateEpochString(bytes, tag779);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;

        //construct low limit message
        copyToFinal(BlockByteParser.lowLimitB);
        copyToFinal(tag1148);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;

        //construct high limit message
        copyToFinal(BlockByteParser.highLimitB);
        copyToFinal(tag1149);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;

        copyToFinal(BlockByteParser.limitPriceB);
        ComputeLimitRangeString(bytes, tag1148, tag1149);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;

        copyToFinal(BlockByteParser.tradingB);
        copyToFinal(tag1150);
        this.outputBytes[outputIdx++] = BlockByteParser.newLine;

    }

    private void copyToFinal(byte[] byteA)
    {
        for(int i = 0; i < byteA.length; i++)
            this.outputBytes[outputIdx++] = byteA[i];
    }
    private void copyToFinal(KeyValueOffsets offset)
    {
        if(offset == null)
        {
            outputNull();
        }else {
            for (int i = offset.getValueStart(); i < offset.getValueEnd(); i++)
                this.outputBytes[outputIdx++] = this.bytes[i];
        }
    }

    private void ToDateEpochString(byte[] dateB, KeyValueOffsets dateO) {
        try {
            int year = parseInt(dateB, dateO.getValueStart(), dateO.getValueStart() + 4);
            int month = parseInt(dateB, dateO.getValueStart() + 4, dateO.getValueStart() + 6);
            int date = parseInt(dateB, dateO.getValueStart() + 6, dateO.getValueStart() + 8);
            int hour = parseInt(dateB, dateO.getValueStart() + 8, dateO.getValueStart() + 10);
            int minute = parseInt(dateB, dateO.getValueStart() + 10, dateO.getValueStart() + 12);
            int second = parseInt(dateB, dateO.getValueStart() + 12, dateO.getValueStart() + 14);
            cal.set(year, month, date, hour, minute, second);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        //java calender does milliseconds since epoch not microseconds
        //could pull the microseconds from the field
        long dateFromEpoch = cal.getTimeInMillis()*1000;
        long power =1000000000000000L;
        for(int i = 0; i < 16; i++)
        {
            this.outputBytes[outputIdx++] = (byte)((dateFromEpoch/power)+48);
            dateFromEpoch %= power;
            power /= 10;
        }
    }

    private void outputNull()
    {
        for(int i = 0; i < this.nullB.length; i++)
            this.outputBytes[outputIdx++] = this.nullB[i];
    }

    private void ComputeLimitRangeString(byte[] bytes, KeyValueOffsets lowPrice, KeyValueOffsets highPrice) throws Exception {
        if (lowPrice == null || highPrice == null) {
            outputNull();
        }else {
            //copy the high price into a temp buffer to do the subtraction in
            byte[] highPriceB = new byte[highPrice.getValueEnd() - highPrice.getValueStart()];
            for (int i = 0; i + highPrice.getValueStart() < highPrice.getValueEnd(); i++)
                highPriceB[i] = bytes[i + highPrice.getValueStart()];

            //determine if the low price is negative or not
            //if it is negative just add the two long values
            int highIdx = highPriceB.length - 1;
            if(bytes[lowPrice.getValueStart()] == BlockByteParser.minus)
            {
                //low value is negative so add both fields
                for (int i = lowPrice.getValueEnd() - 1; i > lowPrice.getValueStart(); i--) {
                    //handle when the negative number is larger than the high price
                    if(highIdx < 0) {
                        highPriceB = addByteAtBeginning(highPriceB);
                        highIdx++;
                    }

                    if (highPriceB[highIdx] != BlockByteParser.period) {
                        highPriceB[highIdx] = (byte)(highPriceB[highIdx] + bytes[i] - BlockByteParser.zero);
                        if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] > BlockByteParser.nine) {
                            if(highIdx == 0) {
                                highPriceB = addByteAtBeginning(highPriceB);
                                highIdx++;
                            }
                            addTen(highPriceB, highIdx);
                        }
                    }
                    highIdx--;
                }
                while(highIdx >= 0)
                {
                    if (highPriceB[highIdx] != BlockByteParser.period && highPriceB[highIdx] != 0x00 && highPriceB[highIdx] > BlockByteParser.nine) {
                        if(highIdx == 0) {
                            highPriceB = addByteAtBeginning(highPriceB);
                            highIdx++;
                        }
                        addTen(highPriceB, highIdx);
                    }
                    highIdx--;
                }

            }else {
                //low value is positive so subtract both fields
                for (int i = lowPrice.getValueEnd() - 1; i >= lowPrice.getValueStart(); i--) {
                    if (highPriceB[highIdx] != BlockByteParser.period) {
                        if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] < BlockByteParser.zero) {
                            borrowTen(highPriceB, highIdx);
                        }
                        highPriceB[highIdx] = (byte) ((highPriceB[highIdx] - bytes[i]) + BlockByteParser.zero);
                        if (highPriceB[highIdx] != 0x00 && highPriceB[highIdx] < BlockByteParser.zero) {
                            borrowTen(highPriceB, highIdx);
                        }
                    }
                    highIdx--;
                }
            }
            for (int i = 0; i < highPriceB.length; i++) {
                this.outputBytes[outputIdx++] = highPriceB[i];
            }
        }
    }

    private byte[] addByteAtBeginning(byte[] bytes)
    {
        byte[] temp = new byte[bytes.length+1];
        temp[0] = BlockByteParser.zero;
        for(int k = 1; k < bytes.length+1; k++)
            temp[k] = bytes[k-1];
        return temp;
    }

    private void addTen(byte[] bytes, int idx)
    {
        if(bytes[idx-1] != BlockByteParser.period)
            bytes[idx - 1] = (byte) (bytes[idx - 1] + 1);
        else
            bytes[idx - 2] = (byte) (bytes[idx - 2] + 1);
        bytes[idx] = (byte) (bytes[idx] - 10);
    }

    private void borrowTen(byte[] bytes, int idx)
    {
        if(bytes[idx-1] != BlockByteParser.period)
            bytes[idx - 1] = (byte) (bytes[idx - 1] - 1);
        else
            bytes[idx - 2] = (byte) (bytes[idx - 2] - 1);
        bytes[idx] = (byte) (bytes[idx] + 10);
    }

    private int getFields(byte[] bytes, int byteOffset, int byteMax, ArrayList<KeyValueOffsets> fields)
    {
        KeyValueOffsets offset = new KeyValueOffsets();
        offset.keyStart = byteOffset;
        for(int i = byteOffset; i < byteMax; i++)
        {
            if(bytes[i] == equals)
            {
                offset.keyEnd = i;
                offset.setValueStart(i+1);
            }else if(bytes[i] == delim)
            {
                offset.setValueEnd(i);
                fields.add(offset);
                offset = new KeyValueOffsets();
                offset.keyStart = i+1;
            }if(bytes[i] == newLine)
            return i+1;
        }
        return byteOffset;
    }

    private int parseInt(byte[] value, int start, int end)
    {
        int ret = 0;
        int power = 1;
        for(int i = end-1; i >= start; i--)
        {
            ret += ((value[i] - zero) *power);
            power *= 10;
        }
        return ret;
    }

    private long parseLong(byte[] value, int start, int end, boolean skipPeriod)
    {
        long ret = 0L;
        long power = 1;
        for(int i = end-1; i >= start; i--)
        {
            if(skipPeriod && value[i] == period)
                continue;
            ret += ((value[i] - zero) *power);
            power *= 10;
        }
        return ret;
    }


}
