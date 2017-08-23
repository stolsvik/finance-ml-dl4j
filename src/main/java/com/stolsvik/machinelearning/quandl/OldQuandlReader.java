package com.stolsvik.machinelearning.quandl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OldQuandlReader {

    private static final Logger log = LoggerFactory.getLogger(OldQuandlReader.class);

    public static final String QUANDL_WIKI_ZIP_DIRECTORY = "/quandl_wiki";

    private static final String POISON_PILL = "die!";
    private static final String[] POISON_PILL_2 = new String[]{"die!!"};

    public static void main(String... args) throws IOException {
        List<String> files = getResourceFiles(QUANDL_WIKI_ZIP_DIRECTORY);
        if (files.size() > 1) {
            throw new IllegalStateException("More files that expected in the quandl_wiki classpath directory" +
                    " (should be 1): " + files);
        }
        String file = files.get(0);
        log.info("File to read: " + file);
        InputStream quandlZipFile = OldQuandlReader.class.getResourceAsStream(QUANDL_WIKI_ZIP_DIRECTORY + '/' + file);
        ZipInputStream zis = new ZipInputStream(quandlZipFile);
        ZipEntry firstEntry = zis.getNextEntry();
        log.info("First entry in zip:" + firstEntry);

        BlockingQueue<String> splitQueue = new ArrayBlockingQueue<>(100000);
        BlockingQueue<String[]> processQueue = new ArrayBlockingQueue<>(100000);

        Runnable splitConsumer = () -> {
            log.info("SplitConsumer started.");
            long startNanos = System.nanoTime();
            long lines = 0;
            try {
                while (true) {
                    String line = splitQueue.take();
                    if (line == POISON_PILL) {
                        splitQueue.put(line);
                        break;
                    }
                    lines++;
                    String[] split = line.split(",");
                    processQueue.put(split);
                }
                processQueue.put(POISON_PILL_2);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted.", e);
            }
            log.info("Lines: [" + lines + "], time taken: [" + ((System.nanoTime() - startNanos) / 1_000_000) + " ms].");
        };

        Thread splitThread = new Thread(splitConsumer, "Split Consumer");
        splitThread.start();

        Runnable processConsumer = () -> {
            log.info("ProcessConsumer started.");
            long startNanos = System.nanoTime();
            long lines = 0;
            try {
                while (true) {
                    String[] splitted = processQueue.take();
                    if (splitted == POISON_PILL_2) {
                        processQueue.put(splitted);
                        break;
                    }
                    lines++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted.", e);
            }
            log.info("Lines: [" + lines + "], time taken: [" + ((System.nanoTime() - startNanos) / 1_000_000) + " ms].");
        };

        Thread processThread = new Thread(processConsumer, "Process Consumer");
        processThread.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(zis), 1024 * 1024);
        long startNanos = System.nanoTime();
        long lines = 0;
        while (true) {
            String line = br.readLine();
            if (line == null) {
                break;
            }
            if (lines < 10) {
                System.out.println(line);
            }
            try {
                splitQueue.put(line);
            } catch (InterruptedException e) {
                throw new RuntimeException("Didn't expect interrupt.", e);
            }
            lines++;
        }
        try {
            splitQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
            throw new RuntimeException("Didn't expect interrupt.", e);
        }

        log.info("Lines: [" + lines + "], time taken: [" + ((System.nanoTime() - startNanos) / 1_000_000) + " ms].");
        try {
            splitThread.join();
            processThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException("Didn't expect interrupt.", e);
        }
        log.info("Consumer joined, exiting.");
        // CSV Header:
        // ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
    }

    private static void processLine(String line) {
        String[] split = line.split(",");
        // log.info("Here it is: "+ Arrays.asList(split));
    }


    /**
     * From https://stackoverflow.com/a/3923685/39334
     */
    private static List<String> getResourceFiles(String path) throws IOException {
        List<String> filenames = new ArrayList<>();
        try (InputStream in = OldQuandlReader.class.getResourceAsStream(path);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String resource;
            while ((resource = br.readLine()) != null) {
                filenames.add(resource);
            }
        }
        return filenames;
    }
}
