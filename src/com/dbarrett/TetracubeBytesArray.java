package com.dbarrett;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by dbarrett on 11/14/16.
 */
public class TetracubeBytesArray {
    byte newLine = 0x0A;
    public static int bufferSize = 81920;

    public TetracubeBytesArray()
    {
    }
    public void run(String input, String output) {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-1);
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(input));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));
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
        String input = "./secdef.dat";
        String output = "./secdef_parsed.txt";
        if(args.length < 1)
        {
            input = args[0];
            output = args[1];
        }
        Long start = System.currentTimeMillis();
        TetracubeBytesArray cube = new TetracubeBytesArray();
        cube.run(input, output);
        Long end = System.currentTimeMillis();
        System.out.println("Total Time " + (end - start) / 1000 + " milli " + (end - start));
    }
}

