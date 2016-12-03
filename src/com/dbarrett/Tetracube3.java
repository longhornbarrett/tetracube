package com.dbarrett;

import java.io.*;
import java.nio.ByteBuffer;
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
    public Tetracube3()
    {
    }
    public void run(String input, String output) {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-1);
            final List<Future<ByteOutput>> futures = new ArrayList<Future<ByteOutput>>();
            FileChannel inputFile = new RandomAccessFile(input, "r").getChannel();
            FileChannel outputFile = new RandomAccessFile(output, "rw").getChannel();
            long fileSize = inputFile.size();
            MappedByteBuffer in = inputFile.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            int currentOffset = 0;
            blockSize = (int)inputFile.size()/(Runtime.getRuntime().availableProcessors()-1);
            while(currentOffset < fileSize)
            {
                long endOfBlock = currentOffset+blockSize;
                if(endOfBlock >= fileSize)
                    endOfBlock = fileSize;
                int idx = (int)--endOfBlock;
                while(in.get(idx)!=newLine)
                    idx--;
                futures.add(executor.submit(new MappedBlockByteParser(in, currentOffset, ++idx)));
                currentOffset = idx;
            }
            for (Future<ByteOutput> future : futures) {
                try {
                    ByteOutput bytes = future.get();
                    outputFile.write(bytes.buffer);
                    //outputFile.write(bytes.buffer, 0, bytes.length);
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

    public void run2(String input, String output) {
        try{
            FileChannel inputFile = new RandomAccessFile(input, "rw").getChannel();
//            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(output));
            //FileChannel outputFile = new RandomAccessFile(output, "rw").getChannel();
            MappedByteBuffer in = inputFile.map(FileChannel.MapMode.READ_WRITE, 0, inputFile.size());
            System.out.println(in.position());
            long initial = in.getLong();
            System.out.println(in.position());
            byte by = in.get();
            System.out.println(in.position());
            int cnt = 0;
            byte one = 0x31;
            long fileSize = inputFile.size();

            for(int i = 0; i < 77824000; i++)
                in.put(i, one);
            //outputFile.transferFrom(inputFile, 0, inputFile.size());
            inputFile.truncate(77824000);

            //in.limit(77824000);
            //in.flip();

            //outputFile.write(in);
            //outputFile.close();
            inputFile.close();
            File inputF = new File(input);
            inputF.renameTo(new File(output));
            //outputFile.close();
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        String input = "./secdef.dat";
        String output = "./secdef_parsed.txt";
        if(args.length > 1)
        {
            input = args[0];
            output = args[1];
        }
        //long flag = 0x000000FF;
        //long test = 3616444609601810486L;
        //System.out.println(test&flag);
        Tetracube3 cube = new Tetracube3();
        long start = System.currentTimeMillis();
        cube.run(input, output);
        long end = System.currentTimeMillis();
        System.out.println("Total Time run " + (end - start) / 1000 + " milli " + (end - start));
    }
}
