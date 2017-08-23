package com.stolsvik.machinelearning.quandl

import com.stolsvik.machinelearning.quandl.QuandlReader.DataPoint
import com.stolsvik.machinelearning.quandl.QuandlReader.Ticker
import groovy.transform.CompileStatic

com.stolsvik.machinelearning.tools.ExploratoryGroovyLooper.loop this, {
    QuandlReader reader = new QuandlReader()
    reader.parseQuandlZipFile()
    _tickerMap = reader.tickerMap
}

// Skip over to @CompileStatic'ed code..
runStatic(_tickerMap)

@CompileStatic
void runStatic(Map<String, Ticker> tickerMap) {
    println "TickerMap.keys ${tickerMap.keySet()}"
    Ticker first = tickerMap.values().first()
    List<DataPoint> points = first.dataPoints

    tickerMap.values().forEach { ticker ->
        double[] adjustedCloses = new double[points.size()]
        for (int i = 0; i < points.size(); i++) {
            adjustedCloses[i] = points[i].adjustedClose
        }
    }

}