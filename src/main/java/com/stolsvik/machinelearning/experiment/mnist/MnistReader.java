package com.stolsvik.machinelearning.experiment.mnist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author Endre Stølsvik, http://endre.stolsvik.com, 2017-03-14 20:22
 */
public class MnistReader {

    private static final Logger log = LoggerFactory.getLogger(MnistReader.class);

    public enum MnistFile {
        TRAINING_IMAGES("mnist/train-images-idx3-ubyte.gz"),

        TRAINING_LABELS("mnist/train-labels-idx1-ubyte.gz"),

        TEST_IMAGES("mnist/t10k-images-idx3-ubyte.gz"),

        TEST_LABELS("mnist/t10k-labels-idx1-ubyte.gz");

        private String _filename;

        MnistFile(String filename) {
            _filename = filename;
        }

        public String getFilename() {
            return _filename;
        }
    }

    public static void main(String[] args) throws IOException {
        byte[][] bytes = readTrainingFile(MnistFile.TRAINING_IMAGES);
        assertInt(60_000, bytes.length);
        byte[][] bytes1 = readTrainingFile(MnistFile.TEST_IMAGES);
        assertInt(10_000, bytes1.length);
        int[] ints = readLabelsFile(MnistFile.TRAINING_LABELS);
        assertInt(60_000, ints.length);
        int[] ints1 = readLabelsFile(MnistFile.TEST_LABELS);
        assertInt(10_000, ints1.length);
    }


    static byte[][] readTrainingFile(MnistFile file) {
        log.debug("Reading file [" + file + "].");
        InputStream stream = getStream(file);
        try {
            long nanoStart = System.nanoTime();
            assertInt(0, stream.read());
            assertInt(0, stream.read());
            assertInt(8, stream.read()); // Bytes
            assertInt(3, stream.read()); // 3 dimensions (array-of-images, which is 28x28 array-of-arrays)
            assertInt(0, stream.read()); // size of dimension 1
            assertInt(0, stream.read()); // size of dimension 1
            int big = stream.read(); // size of dimension 1
            int small = stream.read(); // size of dimension 1: i.e. 235*256 + 96 = 60000
            assertInt(0, stream.read()); // size of dimension 2
            assertInt(0, stream.read()); // size of dimension 2
            assertInt(0, stream.read()); // size of dimension 2
            assertInt(28, stream.read()); // size of dimension 2: 28 pixels high
            assertInt(0, stream.read()); // size of dimension 3
            assertInt(0, stream.read()); // size of dimension 3
            assertInt(0, stream.read()); // size of dimension 3
            assertInt(28, stream.read()); // size of dimension 3: 28 pixels wide

            int size = big * 256 + small;
            byte[][] images = new byte[size][];
            for (int i = 0; i < size; i++) {
                byte[] image = new byte[28 * 28];
                stream.read(image);
                images[i] = image;
            }
            assertInt(-1, stream.read());

            log.debug("  \\- took " + timingFrom(nanoStart) + ".");
            return images;
        } catch (IOException e) {
            throw new AssertionError("Something wrong with the file [" + file + "].", e);
        }
    }

    static int[] readLabelsFile(MnistFile file) {
        log.debug("Reading file [" + file + "].");
        InputStream stream = getStream(file);
        try {
            long nanoStart = System.nanoTime();
            assertInt(0, stream.read());
            assertInt(0, stream.read());
            assertInt(8, stream.read()); // Bytes
            assertInt(1, stream.read()); // 1 dimension: the labels
            assertInt(0, stream.read()); // size of dimension 1
            assertInt(0, stream.read()); // size of dimension 1
            int big = stream.read(); // size of dimension 1
            int small = stream.read(); // size of dimension 1: i.e. 235*256 + 96 = 60000

            int size = big * 256 + small;
            int[] labels = new int[size];
            for (int i = 0; i < size; i++) {
                labels[i] = stream.read();
            }
            assertInt(-1, stream.read());
            log.debug("  \\- took " + timingFrom(nanoStart) + ".");
            return labels;
        } catch (IOException e) {
            throw new AssertionError("Something wrong with the file [" + file + "].", e);
        }
    }

    private static BufferedInputStream getStream(MnistFile file) {
        InputStream gzStream = MnistReader.class.getResourceAsStream('/' + file.getFilename());
        try {
            return new BufferedInputStream(new GZIPInputStream(gzStream));
        } catch (IOException e) {
            throw new AssertionError("Didn't get file [" + file + "].", e);
        }
    }

    private static void assertInt(int expected, int actual) {
        if (actual != expected) {
            throw new AssertionError("Expected " + expected + ", while actual was " + actual + ".");
        }
    }

    private static String timingFrom(long fromNanos) {
        double millisBetween = (System.nanoTime() - fromNanos) / 1_000_000d;
        return String.format("%.3f ms", millisBetween);
    }
}

