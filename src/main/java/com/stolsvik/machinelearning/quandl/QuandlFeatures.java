package com.stolsvik.machinelearning.quandl;

public class QuandlFeatures {

    public static void main(String[] args) {
        QuandlReader reader = new QuandlReader();
        reader.parseQuandlZipFile();
        reader.getTickerMap();

    }

}
