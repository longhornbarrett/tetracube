package com.dbarrett;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class TetracubeNaive {
    char del = 0x0F;
    static String split = "" + (char) 0x01;
    static NumberFormat numberFormat;
    static DecimalFormat decimal8Digit, decimal6Digit;

    TetracubeNaive() {
          numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(8);
        decimal8Digit = (DecimalFormat)NumberFormat.getNumberInstance();
        decimal8Digit.applyPattern("00000000");
        decimal6Digit = (DecimalFormat)NumberFormat.getNumberInstance();
        decimal6Digit.applyPattern("000000");
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat")));
            String line = null;
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/dbarrett/dev/tetracube/output/secdef_parsed.txt")));
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            while ((line = reader.readLine()) != null) {
                ParseLine(line, sb);
                if (cnt++ % 1000 == 0) {
                    writer.write(sb.toString());
                    sb.setLength(0);
                }
            }
            reader.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String ToDateEpochString(String dateString) {
        Integer year = Integer.parseInt(dateString.substring(0, 4));
        Integer month = Integer.parseInt(dateString.substring(4, 6));
        Integer date = Integer.parseInt(dateString.substring(6, 8));
        Integer hour = Integer.parseInt(dateString.substring(8, 10));
        Integer minute = Integer.parseInt(dateString.substring(10, 12));
        Integer second = Integer.parseInt(dateString.substring(12, 14));
        Integer us = Integer.parseInt(dateString.substring(14, 20));
        Calendar cal = new GregorianCalendar(year, month, date, hour, minute, second);
        String result = (cal.getTimeInMillis() / 1000) + decimal6Digit.format(us);
        return result;
    }

    private String ComputeLimitRangeString(String lowPrice, String highPrice) throws Exception {
        if (lowPrice == null || highPrice == null) return "NULL";
        if (lowPrice.charAt(lowPrice.length() - 8) != '.') throw new Exception("Bug");
        if (highPrice.charAt(highPrice.length() - 8) != '.') throw new Exception("Bug");
        lowPrice = lowPrice.replace(".", "");
        highPrice = highPrice.replace(".", "");
        Long lLowPrice = Long.parseLong(lowPrice);
        Long lHighPrice = Long.parseLong(highPrice);
        Long lRange = lHighPrice - lLowPrice;
        String sRange = decimal8Digit.format(lRange);
        String result = "";
        try {
             result = sRange.substring(0, sRange.length() - 7) + "." + sRange.substring(sRange.length() - 7);
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return result;
    }

    public void ParseLine(String line, StringBuilder sb) throws Exception {
        if (IsNullOrWhiteSpace(line)) return;
        String tag48 = null;
        String tag55 = null;
        String tag779 = null;
        String tag1148 = null;
        String tag1149 = null;
        String tag1150 = null;
        String[] fields = line.split(split);
        for (String field : fields) {
            if (IsNullOrWhiteSpace(field)) continue;
            String[] tv = field.split("=");
            if (tv.length != 2) continue;
            String tagString = tv[0];
            String valueString = tv[1];
            if (tagString.equals("48"))
                tag48 = valueString;
            else if (tagString.equals("55"))
                tag55 = valueString;
            else if (tagString.equals("779"))
                tag779 = valueString;
            else if (tagString.equals("1148"))
                tag1148 = valueString;
            else if (tagString.equals("1149"))
                tag1149 = valueString;
            else if (tagString.equals("1150"))
                tag1150 = valueString;
        }
        sb.append(tag48 + ":" + tag55 + "\n");
        sb.append("\tLastUpdateTime=" + ToDateEpochString(tag779) + "\n");
        sb.append("\tLowLimitPrice=" + tag1148 + "\n");
        sb.append("\tHighLimitPrice=" + tag1149 + "\n");
        sb.append("\tLimitPriceRange=" + ComputeLimitRangeString(tag1148, tag1149) + "\n");
        sb.append("\tTradingReferencePrice=" + tag1150 + "\n");
    }

    public static boolean IsNullOrWhiteSpace(String field) {
        if (field != null && !field.equals("")) return false;
        return true;
    }

    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        TetracubeNaive cube = new TetracubeNaive();
        cube.run();
        Long end = System.currentTimeMillis();
        System.out.println("Total Time " +(end - start)/1000 + " milli "+(end-start));
    }
}
