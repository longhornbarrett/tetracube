package com.dbarrett;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by dbarrett on 11/13/16.
 */
public class TetracubeFast {


    public void run() {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-1);
            //BufferedInputStream stream = new BufferedInputStream(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/dbarrett/dev/tetracube/input/secdef.dat")));
            String line = null;
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/dbarrett/dev/tetracube/output/secdef_parsed.txt")));
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            ArrayList<String> rows = new ArrayList<String>();
            final List<Future<String>> futures = new ArrayList<Future<String>>();
            while ((line = reader.readLine()) != null) {
                rows.add(line);
                if (cnt++ % 50 == 0) {
                    final Callable<String> worker = new BlockParser(rows);
                    futures.add(executor.submit(worker));
                    rows = new ArrayList<String>();
                }
            }
            final Callable<String> worker = new BlockParser(rows);
            futures.add(executor.submit(worker));
            for (Future<String> future : futures) {
                try {
                    writer.write(future.get());
                } catch (final ExecutionException e) {
                    e.printStackTrace();
                    System.out.println("Error during processing");
                }
            }
            reader.close();
            writer.close();
            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        TetracubeFast cube = new TetracubeFast();
        cube.run();
        Long end = System.currentTimeMillis();
        System.out.println("Total Time " + (end - start) / 1000 + " milli " + (end - start));
    }
}
