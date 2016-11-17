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
    byte delim = 0x01;
    byte equals = 0x3D;
    byte newLine = 0x0A;
    byte period = 0x2E;
    byte zero = 0x30;
    byte one = 0x31;
    byte four = 0x34;
    byte five = 0x35;
    byte seven = 0x37;
    byte eight = 0x38;
    byte nine = 0x39;
    byte semi = 0x3A;
    char newLineC = '\n';
    byte[] lastUpdateB = {0x09, 0x4C, 0x61, 0x73, 0x74, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6D, 0x65, 0x3D,};
    byte[] lowLimitB = { 0x09, 0x4C, 0x6F, 0x77, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    byte[] highLimitB = { 0x09, 0x48, 0x69, 0x67, 0x68, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    byte[] limitPriceB = {0x09, 0x4C, 0x69, 0x6D, 0x69, 0x74, 0x50, 0x72, 0x69, 0x63, 0x65, 0x52, 0x61, 0x6E, 0x67, 0x65, 0x3D};
    byte[] tradingB = {0x09, 0x54,  0x72, 0x61, 0x64, 0x69, 0x6E, 0x67, 0x52, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x50, 0x72, 0x69, 0x63, 0x65, 0x3D};
    byte[] nullB = {0x4E,0x55,0x4C,0x4C};
    Calendar cal = new GregorianCalendar();
    byte[] bytes;
    byte[] outputBytes;
    int outputIdx = 0;
    int nRead;

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
    public void parseLineNoCopy(byte[] bytes, List<KeyValueOffsets> keys) throws Exception
    {
        KeyValueOffsets tag48 = null;
        KeyValueOffsets tag55 = null;
        KeyValueOffsets tag779 = null;
        KeyValueOffsets tag1148 = null;
        KeyValueOffsets tag1149 = null;
        KeyValueOffsets tag1150 = null;
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
        this.outputBytes[outputIdx++] = this.semi;
        copyToFinal(tag55);
        this.outputBytes[outputIdx++] = this.newLine;
        //construct the date message

        copyToFinal(lastUpdateB);
        ToDateEpochString(bytes, tag779);
        this.outputBytes[outputIdx++] = newLine;
        //construct low limit message
        copyToFinal(this.lowLimitB);
        copyToFinal(tag1148);
        this.outputBytes[outputIdx++] = newLine;
        //construct high limit message
        copyToFinal(highLimitB);
        copyToFinal(tag1149);
        this.outputBytes[outputIdx++] = newLine;

        ComputeLimitRangeString(bytes, tag1148, tag1149);
        copyToFinal(limitPriceB);
        this.outputBytes[outputIdx++] = newLine;

        copyToFinal(tradingB);
        copyToFinal(tag1150);
        this.outputBytes[outputIdx++] = newLine;

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
            for(int i = 0; i < this.nullB.length; i++)
                this.outputBytes[outputIdx++] = this.nullB[i];
        }else {
            for (int i = offset.valueStart; i < offset.valueEnd; i++)
                this.outputBytes[outputIdx++] = this.bytes[i];
        }
    }

    private void ToDateEpochString(byte[] dateB, KeyValueOffsets dateO) {
        try {
            int year = parseInt(dateB, dateO.valueStart, dateO.valueStart + 4);
            int month = parseInt(dateB, dateO.valueStart + 4, dateO.valueStart + 6);
            int date = parseInt(dateB, dateO.valueStart + 6, dateO.valueStart + 8);
            int hour = parseInt(dateB, dateO.valueStart + 8, dateO.valueStart + 10);
            int minute = parseInt(dateB, dateO.valueStart + 10, dateO.valueStart + 12);
            int second = parseInt(dateB, dateO.valueStart + 12, dateO.valueStart + 14);
            cal.set(year, month, date, hour, minute, second);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        long dateFromEpoch = cal.getTimeInMillis()*1000;
        long power =1000000000000000L;
        for(int i = 0; i < 16; i++)
        {
            this.outputBytes[outputIdx++] = (byte)((dateFromEpoch/power)+48);
            dateFromEpoch %= power;
            power /= 10;
        }
    }

    private String ComputeLimitRangeString(byte[] bytes, KeyValueOffsets lowPrice, KeyValueOffsets highPrice) throws Exception {
        if (lowPrice == null || highPrice == null) return "NULL";
        long lLowPrice = parseLong(bytes, lowPrice.valueStart, lowPrice.valueEnd, true);
        long lHighPrice = parseLong(bytes, highPrice.valueStart, highPrice.valueEnd, true);
        long lRange = lHighPrice - lLowPrice;
        String sRange = String.valueOf(lRange);
        String result = "";
        try {
            int slength = sRange.length();
            int dec = slength-7;
            char[] cResult = new char[slength+1];
            int targetIdx = 0;
            if(dec <= 0)
                result = "0."+sRange;
            else {
                for (int i = 0; i < slength; i++) {
                    if (i == dec)
                        cResult[targetIdx++] = '.';
                    cResult[targetIdx++] = sRange.charAt(i);
                }
                result = new String(cResult);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    private int getFields(byte[] bytes, int byteOffset, int byteMax, ArrayList<KeyValueOffsets> fields)
    {
        KeyValueOffsets offset = new KeyValueOffsets();
        offset.keyStart = 0+byteOffset;
        for(int i = 0+byteOffset; i < byteMax; i++)
        {
            if(bytes[i] == equals)
            {
                offset.keyEnd = i;
                offset.valueStart = i+1;
            }else if(bytes[i] == delim)
            {
                offset.valueEnd = i;
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
