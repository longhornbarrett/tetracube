package com.dbarrett;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by dbarrett on 11/14/16.
 */
public class BlockByteParser implements Callable<String> {
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
    char semi = ':';
    char newLineC = '\n';
    String lastUpdate = "\tLastUpdateTime=";
    String lowLimit = "\tLowLimitPrice=";
    String highLimit = "\tHighLimitPrice=";
    String limitPrice = "\tLimitPriceRange=";
    String trading = "\tTradingReferencePrice=";
    Calendar cal = new GregorianCalendar();
    StringBuilder sb = new StringBuilder(TetracubeBytesArray.bufferSize);
    byte[] bytes;
    int nRead;

    public BlockByteParser(byte[] buffer, int nRead)
    {
        this.bytes = buffer;
        this.nRead = nRead;
    }

    @Override
    public String call() throws Exception {
        int j = 0;
        ArrayList<KeyValueOffsets> fields = new ArrayList<KeyValueOffsets>();
        for(int i = 0; j < nRead; i = j) {
            fields.clear();
            j = getFields(bytes, i, nRead, fields);
            if(j == i)
            {
                break;
            }
            parseLineNoCopy(bytes, fields, sb);
        }
         return sb.toString();
    }
    public void parseLineNoCopy(byte[] bytes, List<KeyValueOffsets> keys, StringBuilder sb) throws Exception
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
        sb.append(getStringValue(bytes, tag48)).append(semi).append(getStringValue(bytes, tag55)).append(newLineC);
        sb.append(lastUpdate).append(ToDateEpochString(bytes, tag779)).append(newLineC);
        sb.append(lowLimit).append(getStringValue(bytes, tag1148)).append(newLineC);
        sb.append(highLimit).append(getStringValue(bytes, tag1149)).append(newLineC);
        sb.append(limitPrice).append(ComputeLimitRangeString(bytes, tag1148, tag1149)).append(newLineC);
        sb.append(trading).append(getStringValue(bytes, tag1150)).append(newLineC);
    }

    private String getStringValue(byte[] bytes, KeyValueOffsets offset)
    {
        if(offset == null)
            return "";
        return new String(bytes, offset.valueStart, offset.getValueLength());
    }

    private String ToDateEpochString(byte[] dateB, KeyValueOffsets dateO) {
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
        String result = String.valueOf(cal.getTimeInMillis() * 1000);
        return result;
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
