package com.stolsvik.machinelearning.quandl;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Reads a "WIKI PRICES" file into memory.
 *
 * @author Endre Stølsvik - http://endre.stolsvik.com/ - 2017-08-23 21:12
 */
public class QuandlReader {

    private static final Logger log = LoggerFactory.getLogger(QuandlReader.class);

    public static final String QUANDL_WIKI_ZIP_DIRECTORY = "/quandl_wiki";

    private Map<String, Ticker> _tickerMap = new TreeMap<>();
    private Map<LocalDate, List<DataPoint>> _dateListMap = new TreeMap<>();

    public Map<String, Ticker> getTickerMap() {
        return _tickerMap;
    }

    public Map<LocalDate, List<DataPoint>> getDateListMap() {
        return _dateListMap;
    }

    public static class Ticker {
        private String tickerName;

        private List<DataPoint> _dataPoints = new ArrayList<>(1024 + 512);

        private Ticker(String tickerName) {
            this.tickerName = tickerName;
        }

        public String getTickerName() {
            return tickerName;
        }

        public List<DataPoint> getDataPoints() {
            return _dataPoints;
        }
    }

    public static class DataPoint {
        private Ticker ticker;
        private LocalDate date;
        private double adj_open;
        private double adj_high;
        private double adj_low;
        private double adj_close;
        private double adj_volume;

        public Ticker getTicker() {
            return ticker;
        }

        public LocalDate getDate() {
            return date;
        }

        public double getAdjustedOpen() {
            return adj_open;
        }

        public double getAdjustedHigh() {
            return adj_high;
        }

        public double getAdjustedLow() {
            return adj_low;
        }

        public double getAdjustedClose() {
            return adj_close;
        }

        public double getAdjustedVolume() {
            return adj_volume;
        }
    }

    private Map<String, LocalDate> _stringLocalDateMap = new TreeMap<>();

    private class LineEvent {
        private String _line;
        private String[] _splitted;
        private DataPoint _dataPoint;

        void setLine(String line) {
            _line = line;
        }

        void a_splitLine() {
            _splitted = _line.split(",");
            // CSV Header:
            // 0      1    2    3    4   5     6      7           8           9        10       11      12        13
            // ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
            // Won't need these strings, so null them out:
            _splitted[2] = null;
            _splitted[3] = null;
            _splitted[4] = null;
            _splitted[5] = null;
            _splitted[6] = null;
            _splitted[7] = null;  // ex-dividend <- is this interesting?
            _splitted[8] = null;  // split_ratio <- is this interesting?
        }

        void b_makeDataPont() {
            _dataPoint = new DataPoint();
            Ticker ticker = _tickerMap.computeIfAbsent(_splitted[0], Ticker::new);
            ticker._dataPoints.add(_dataPoint);
            _dataPoint.ticker = ticker;
        }

        void c_parseNumbers1() {
            // CSV Header:
            // 0      1    2    3    4   5     6      7           8           9        10       11      12        13
            // ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
            try {
                _dataPoint.adj_open = Double.parseDouble(_splitted[9]);
                _splitted[9] = null;
                _dataPoint.adj_high = Double.parseDouble(_splitted[10]);
                _splitted[10] = null;
                _dataPoint.adj_low = Double.parseDouble(_splitted[11]);
                _splitted[11] = null;

            }
            catch (NumberFormatException e) {
                log.warn("Got NFE when parsing [" + _splitted[0] + "] [" + _splitted[1] + "] - full line:\n" + _line, e);
            }
        }

        void d_parseNumbers2() {
            // CSV Header:
            // 0      1    2    3    4   5     6      7           8           9        10       11      12        13
            // ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
            try {
                _dataPoint.adj_close = Double.parseDouble(_splitted[12]);
                _splitted[12] = null;
                _dataPoint.adj_volume = Double.parseDouble(_splitted[13]);
                _splitted[13] = null;
            }
            catch (NumberFormatException e) {
                log.warn("Got NFE when parsing [" + _splitted[0] + "] [" + _splitted[1] + "] - full line:\n" + _line, e);
            }
            _line = null;

        }

        void e_parseDate() {
            // CSV Header:
            // 0      1    2    3    4   5     6      7           8           9        10       11      12        13
            // ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
            LocalDate date = _stringLocalDateMap.computeIfAbsent(_splitted[1], s -> LocalDate.parse(s));
            _dataPoint.date = date;

            // Null out the splitted-array.
            _splitted = null;

            List<DataPoint> dataPointsForDate = _dateListMap.computeIfAbsent(_dataPoint.date, s -> new ArrayList<>(256));
            dataPointsForDate.add(_dataPoint);

            // Null out the DataPoint from the Event instance, it is now tucked away where it should be.
            _dataPoint = null;
        }
    }


    static class LineHolder {
        String line;
    }

    public static void main(String[] args) throws IOException {
        QuandlReader reader = new QuandlReader();
        reader.parseQuandlZipFile();
    }

    public void parseQuandlZipFile() {
        BufferedReader br;
        try {
            List<String> files = getResourceFiles(QUANDL_WIKI_ZIP_DIRECTORY);
            if (files.size() > 1) {
                throw new IllegalStateException("More files than expected in the quandl_wiki classpath directory" +
                        " (should be 1): " + files);
            }
            String file = files.get(0);
            log.info("File to read: " + file);
            InputStream quandlZipFile = QuandlReader.class.getResourceAsStream(QUANDL_WIKI_ZIP_DIRECTORY + '/' + file);
            ZipInputStream zis = new ZipInputStream(quandlZipFile);
            ZipEntry firstEntry = zis.getNextEntry();
            log.info("First entry in zip:" + firstEntry);
            br = new BufferedReader(new InputStreamReader(zis), 1024 * 1024);
            // Kill the header line
            log.info("HEADER: " + br.readLine());
        }
        catch (IOException ioE) {
            throw new IllegalStateException("Couldn't find the Quandl WIKI_PRICES file.", ioE);
        }


        log.info("Creating Disruptor for multi-step multi-thread parsing.");

        Disruptor<LineEvent> disruptor = new Disruptor<>(LineEvent::new, 1024 * 512, r -> {
            return new Thread(r, "ENDRE THREAD");
        }, ProducerType.SINGLE, new SleepingWaitStrategy());

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> event.a_splitLine())
                .then((event, sequence, endOfBatch) -> event.b_makeDataPont())
                .then((event, sequence, endOfBatch) -> event.c_parseNumbers1())
                .then((event, sequence, endOfBatch) -> event.d_parseNumbers2())
                .then((event, sequence, endOfBatch) -> event.e_parseDate());

        disruptor.start();

        RingBuffer<LineEvent> ringBuffer = disruptor.getRingBuffer();

        long startNanos = System.nanoTime();
        long parsedLines = 0;
        long droppedLines = 0;
        LineHolder lineHolder = new LineHolder();
        while (true) {
            String line;
            try {
                line = br.readLine();
            }
            catch (IOException ioE) {
                throw new RuntimeException("Got problems reading Quandl WIKI_PRICES file.", ioE);
            }

            if (line == null) {
                break;
            }
            if (parsedLines % 1_000_000 == 0) {
                log.info("Parsed [" + parsedLines + "] lines, current line: " + line);
            }

            // If any of the tickers with missing values for OHLC, then just skip the entire ticker.
            if (line.startsWith("ATMI")
                    || line.startsWith("BAGL")
                    || line.startsWith("BODY")
                    || line.startsWith("DFZ")
                    || line.startsWith("DFZ")
                    || line.startsWith("EDIG")
                    || line.startsWith("FIO")
                    || line.startsWith("FURX")
                    || line.startsWith("GMT")
                    || line.startsWith("HITK")
                    || line.startsWith("HSH")
                    || line.startsWith("LLEN")
                    || line.startsWith("NASB")
                    || line.startsWith("OPLK")
                    || line.startsWith("OPEN")
                    || line.startsWith("PLXT")
                    || line.startsWith("SUSS")
                    || line.startsWith("SGK")
                    || line.startsWith("TAYC")
                    || line.startsWith("TWGP")
                    || line.startsWith("VITC")
                    || line.startsWith("ZIGO")
                    ) {
                droppedLines++;
                continue;
            }

            lineHolder.line = line;
            ringBuffer.publishEvent((event, sequence, lineArray) -> event.setLine(lineArray.line), lineHolder);

            parsedLines++;

//            if (lines >= 4_000_000) {
//                break;
//            }
        }


        log.info("Lines parsed: [" + parsedLines + "] (dropped lines: [" + droppedLines + "]), time taken: [" + ((System.nanoTime() - startNanos) / 1_000_000) + " ms].");

        disruptor.shutdown();
        log.info("Disruptor shut down, total time taken: [" + ((System.nanoTime() - startNanos) / 1_000_000) + " ms].");

        log.info("Tickers: " + _tickerMap.size() + ": " + _tickerMap.keySet());

        int dataPointCountFromTickers = 0;
        for (Ticker ticker : _tickerMap.values()) {
            dataPointCountFromTickers += ticker._dataPoints.size();
        }
        log.info("Total DataPoints from Tickers: " + dataPointCountFromTickers);

        int dataPointCountFromDates = 0;
        for (List<DataPoint> listForDate : _dateListMap.values()) {
            dataPointCountFromDates += listForDate.size();
        }
        log.info("Total DataPoints from Dates: " + dataPointCountFromDates);
    }

    /**
     * From https://stackoverflow.com/a/3923685/39334
     */
    private static List<String> getResourceFiles(String path) throws IOException {
        List<String> filenames = new ArrayList<>();
        try (InputStream in = QuandlReader.class.getResourceAsStream(path);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String resource;
            while ((resource = br.readLine()) != null) {
                filenames.add(resource);
            }
        }
        return filenames;
    }
}
