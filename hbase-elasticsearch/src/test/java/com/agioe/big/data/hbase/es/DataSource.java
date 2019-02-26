package com.agioe.big.data.hbase.es;

import com.agioe.big.data.hbase.es.hbase.HBaseUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author yshen
 * @since 19-2-26
 */
public class DataSource {
    public static void main(String[] args) throws InterruptedException {
        for (; ; ) {
            long start = System.currentTimeMillis();
            produce();
            System.out.println("Hbase write time：" + (System.currentTimeMillis() - start));

            Thread.sleep(5000);
        }


    }


    private static void produce() {

        int prefix = 100000;
        List<Put> puts = new ArrayList<>();
        Date date = new Date();
        for (int i = 1; i < 100001; i++) {
            Put put = new Put(Bytes.toBytes(prefix + i + "-" + dateFormat(date)));
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())), Bytes.toBytes(roundNumber()));
            puts.add(put);
        }
        HBaseUtil.putRows("point_monitor_data", puts);

    }

    private static String dateFormat(Date date) {
        return DateFormat.getDateInstance(DateFormat.DEFAULT).format(date);
    }


    private static String roundNumber() {
        double min = 10.00;
        double max = 30.00;
        double v = new RandomDataGenerator().nextF(min, max);
        return String.valueOf(v);
    }
}
