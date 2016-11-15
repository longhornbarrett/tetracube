package com.dbarrett;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by dbarrett on 11/14/16.
 */
public class TetracubeBytesArray {

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
    static NumberFormat numberFormat;
    static DecimalFormat decimal8Digit, decimal6Digit;
    int bufferSize = 81920;

    public TetracubeBytesArray()
    {
    }
    public void run() {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-1);
            //final ExecutorService executor = Executors.newFixedThreadPool(1);
            BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat"));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/dbarrett/dev/tetracube/output/secdef_parsed.txt")));
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            byte[] buffer = new byte[bufferSize];
            byte[] tempBuffer;
            int nRead;

            int readOffset = 0;
            final List<Future<String>> futures = new ArrayList<Future<String>>();
            while ((nRead = in.read(buffer, readOffset, buffer.length-readOffset)) != -1) {
                nRead += readOffset;
                if(nRead < buffer.length )
                {
                    futures.add(executor.submit(new BlockByteParser(buffer, nRead)));
                }else {
                    if (buffer[buffer.length - 1] == newLine) {
                        futures.add(executor.submit(new BlockByteParser(buffer, nRead)));
                        readOffset = 0;
                        buffer = new byte[bufferSize];
                    } else {
                        for (int i = buffer.length - 1; i >= 0; i--) {
                            if (buffer[i] == newLine) {
                                int k = 0;
                                tempBuffer = new byte[bufferSize];
                                for (int j = i + 1; j < nRead; j++)
                                    tempBuffer[k++] = buffer[j];
                                futures.add(executor.submit(new BlockByteParser(buffer, i)));
                                buffer = tempBuffer;
                                readOffset = k;
                                i = -1;
                            }
                        }
                    }
                }
            }
            for (Future<String> future : futures) {
                try {
                    out.write(future.get());
                } catch (final ExecutionException e) {
                    e.printStackTrace();
                    System.out.println("Error during processing");
                }
            }
            in.close();
            out.close();
            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        TetracubeBytesArray cube = new TetracubeBytesArray();
        cube.run();
        Long end = System.currentTimeMillis();
        System.out.println("Total Time " + (end - start) / 1000 + " milli " + (end - start));
    }
}

