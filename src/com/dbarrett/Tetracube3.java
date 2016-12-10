package com.dbarrett;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by dbarrett on 11/23/16.
 */
public class Tetracube3 {
    public static int blockSize = 81920;
    private static byte newLine = 0x0A;

    public Tetracube3() {
    }

    public void run(String input, String output) {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 1);
            final List<Future<ByteOutput>> futures = new ArrayList<Future<ByteOutput>>();
            FileChannel inputFile = new RandomAccessFile(input, "r").getChannel();
            FileChannel outputFile = new RandomAccessFile(output, "rw").getChannel();
            long fileSize = inputFile.size();
            MappedByteBuffer in = inputFile.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            int currentOffset = 0;
            blockSize = (int) (inputFile.size() / (Runtime.getRuntime().availableProcessors() - 1));
            while (currentOffset < fileSize) {
                long endOfBlock = currentOffset + blockSize;
                if (endOfBlock >= fileSize)
                    endOfBlock = fileSize;
                int idx = (int) --endOfBlock;
                while (in.get(idx) != newLine)
                    idx--;
                futures.add(executor.submit(new MappedBlockByteParser(in, currentOffset, ++idx)));
                currentOffset = idx;
            }
            for (Future<ByteOutput> future : futures) {
                try {
                    ByteOutput bytes = future.get();
                    outputFile.write(bytes.buffer);
                } catch (final ExecutionException e) {
                    e.printStackTrace();
                    System.out.println("Error during processing");
                }
            }
            executor.shutdown();
            inputFile.close();
            outputFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        String input = "./secdef.dat";
        String output = "./secdef_parsed.txt";
        if (args.length > 1) {
            input = args[0];
            output = args[1];
        }

        Tetracube3 cube = new Tetracube3();
        long start = System.currentTimeMillis();
        cube.run(input, output);
        long end = System.currentTimeMillis();

        long start2 = System.currentTimeMillis();
        cube.run(input, output);
        long end2 = System.currentTimeMillis();


        long start3 = System.currentTimeMillis();
        cube.run(input, output);
        long end3 = System.currentTimeMillis();


        long start4 = System.currentTimeMillis();
        cube.run(input, output);
        long end4 = System.currentTimeMillis();


        System.out.println("Total Time run 1 milli " + (end - start));
        System.out.println("JIT now has optimized the code ");
        System.out.println("Total Time run 2 milli " + (end2 - start2));
        System.out.println("Total Time run 3 milli " + (end3 - start3));
        System.out.println("Total Time run 4 milli " + (end4 - start4));
    }
}
