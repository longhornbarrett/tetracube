package com.dbarrett;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by dbarrett on 11/13/16.
 */
public class TetracubeFast {

    char del = 0x0F;
    char lineSplit =(char) 0x01;
    char pairSplit = '=';
    Pattern splitReg = Pattern.compile("" + (char) 0x01);
    static NumberFormat numberFormat;
    static DecimalFormat decimal8Digit, decimal6Digit;
    private long totalParse, totalCompute, totalDate;
    char zero = '0';
    char one = '1';
    char four = '4';
    char five = '5';
    char seven = '7';
    char eight = '8';
    char nine = '9';
    char period = '.';
    char newLine = '\n';
    char semi = ':';
    String lastUpdate = "\tLastUpdateTime=";
    String lowLimit = "\tLowLimitPrice=";
    String highLimit = "\tHighLimitPrice=";
    String limitPrice = "\tLimitPriceRange=";
    String trading = "\tTradingReferencePrice=";
    Calendar cal = new GregorianCalendar();

    TetracubeFast() {
        numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(8);
        decimal8Digit = (DecimalFormat) NumberFormat.getNumberInstance();
        decimal8Digit.applyPattern("00000000");
        decimal6Digit = (DecimalFormat) NumberFormat.getNumberInstance();
        decimal6Digit.applyPattern("000000");
    }

    public void run() {
        try {
            //BufferedInputStream stream = new BufferedInputStream(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat")));
            String line = null;
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/dbarrett/dev/tetracube/output/secdef_parsed.txt")));
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            while ((line = reader.readLine()) != null) {
                ParseLineNoCopy(line, sb);
                if (cnt++ % 100 == 0) {
                    writer.write(sb.toString());
                    sb.setLength(0);
                }
            }
            reader.close();
            writer.close();
            System.out.println("Total Date :" + this.totalDate + " Total Compute :" + this.totalCompute + " Total Parse :" + this.totalParse);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String ToDateEpochString(String dateString) {
        long start = System.currentTimeMillis();
        int year = parseInt(dateString, 0, 4);
        int month = parseInt(dateString, 4, 6);
        int date = parseInt(dateString, 6, 8);
        int hour = parseInt(dateString, 8, 10);
        int minute = parseInt(dateString, 10, 12);
        int second = parseInt(dateString, 12, 14);
        cal.set(year, month, date, hour, minute, second);
        String result = (cal.getTimeInMillis() / 1000) + dateString.substring(14, 20);
        this.totalDate += (System.currentTimeMillis() - start);
        return result;
    }

    private String ComputeLimitRangeString(String lowPrice, String highPrice) throws Exception {
        long start = System.currentTimeMillis();
        if (lowPrice == null || highPrice == null) return "NULL";
        if (lowPrice.charAt(lowPrice.length() - 8) != '.') throw new Exception("Bug");
        if (highPrice.charAt(highPrice.length() - 8) != '.') throw new Exception("Bug");
        long lLowPrice = parseLong(lowPrice, true);
        long lHighPrice = parseLong(highPrice, true);
        long lRange = lHighPrice - lLowPrice;
        String sRange = decimal8Digit.format(lRange);
        String result = "";
        try {
            result = sRange.substring(0, sRange.length() - 7) + "." + sRange.substring(sRange.length() - 7);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        this.totalCompute += (System.currentTimeMillis() - start);
        return result;
    }

    private int parseInt(String value, int start, int end)
    {
        int ret = 0;
        int power = 1;
        for(int i = end-1; i >= start; i--)
        {
            ret += ((value.charAt(i) - zero) *power);
            power *= 10;
        }
        return ret;
    }

    private long parseLong(String value, boolean skipPeriod)
    {
        long ret = 0L;
        long power = 1;
        for(int i = value.length()-1; i >= 0; i--)
        {
            if(skipPeriod && value.charAt(i) == '.')
                continue;
            ret += ((value.charAt(i) - zero) *power);
            power *= 10;
        }
        return ret;
    }

    public void ParseLineNoCopy(String line, StringBuilder sb) throws Exception
    {

        if (IsNullOrWhiteSpace(line)) return;
        String tag48 = null;
        String tag55 = null;
        String tag779 = null;
        String tag1148 = null;
        String tag1149 = null;
        String tag1150 = null;
        List<KeyValueOffsets> keys = getFields(line);
        long start = System.currentTimeMillis();
        for (KeyValueOffsets field : keys) {
            int tagLength = field.getTagLength();
            char f = line.charAt(field.keyStart);
            if (tagLength <= 3) {
                if (tagLength == 2) {
                    if (f == four && line.charAt(field.keyStart+1) == eight)

                        tag48 = line.substring(field.keyEnd+1, field.valueEnd);
                    else if (f == five && line.charAt(field.keyStart+1) == five)
                        tag55 = line.substring(field.keyEnd+1, field.valueEnd);
                } else if (tagLength == 3 && f == seven && line.charAt(field.keyStart+1) == seven && line.charAt(field.keyStart+2) == nine)
                    tag779 = line.substring(field.keyEnd+1, field.valueEnd);
            } else if (tagLength == 4) {
                if (f == one && line.charAt(field.keyStart+1) == one) {
                    if (line.charAt(field.keyStart+2) == four) {
                        if (line.charAt(field.keyStart+3) == eight)
                            tag1148 = line.substring(field.keyEnd+1, field.valueEnd);
                        else if (line.charAt(field.keyStart+3) == nine)
                            tag1149 = line.substring(field.keyEnd+1, field.valueEnd);
                    }
                    else if (line.charAt(field.keyStart+2) == five && line.charAt(field.keyStart+3) == zero)
                        tag1150 = line.substring(field.keyEnd+1, field.valueEnd);
                }
            }
        }
        sb.append(tag48).append(semi).append(tag55).append(newLine);
        sb.append(lastUpdate).append(ToDateEpochString(tag779)).append(newLine);
        sb.append(lowLimit).append(tag1148).append(newLine);
        sb.append(highLimit).append(tag1149).append(newLine);
        sb.append(limitPrice).append(ComputeLimitRangeString(tag1148, tag1149)).append(newLine);
        sb.append(trading).append(tag1150).append(newLine);
        this.totalParse += (System.currentTimeMillis() - start);
    }

    private ArrayList<KeyValueOffsets> getFields(String line)
    {
        ArrayList<KeyValueOffsets> fields = new ArrayList<KeyValueOffsets>();
        KeyValueOffsets offset = new KeyValueOffsets();
        offset.keyStart = 0;
        for(int i = 0; i < line.length(); i++)
        {
            if(line.charAt(i) == pairSplit)
            {
                offset.keyEnd = i;
            }else if(line.charAt(i) == lineSplit)
            {
                offset.valueEnd = i;
                fields.add(offset);
                offset = new KeyValueOffsets();
                offset.keyStart = i+1;
            }
        }
        if(offset.keyStart < line.length() && offset.keyEnd < line.length()) {
            offset.valueEnd = line.length();
            fields.add(offset);
        }
        return fields;
    }

    public static boolean IsNullOrWhiteSpace(String field) {
        if (field != null && field.length() > 0) return false;
        return true;
    }


    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        TetracubeFast cube = new TetracubeFast();
        cube.run();
        Long end = System.currentTimeMillis();
        System.out.println("Total Time " + (end - start) / 1000 + " milli " + (end - start));
    }
}
