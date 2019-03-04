package com.agioe.big.data.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseUtilTest {
    /**
     * 创建表
     */
    @Test
    public void createTable() { //交易记录表
        /* HBaseUtil.createTable("transaction_record_list", new String[]{"fileInfo"});*/

        //客户入金表
        /*HBaseUtil.createTable("customer_in_list", new String[]{"fileInfo"});*/

        //分析结果表
        /*  HBaseUtil.createTable("result_analysis",new String[]{"fileInfo"});*/

        //测试表
        HBaseUtil.createTable("test_list", new String[]{"fileInfo"});
    }

    /**
     * 向表中插入数据
     */
    @Test
    public void addFileDetails() {
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "serial_num", "2");
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "customer_num", "test00004");
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "currency_in", "JPY");
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "amount_in", "9823");
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "date_in", "20180614");
        HBaseUtil.putRow("test_list", "rowkey11", "fileInfo", "time_in", "103007");

    }

    /**
     * 检索数据
     */
    @Test
    public void getFileDetails() {
        Result result = HBaseUtil.getRow("test_list", "rowkey11");
        if (result != null) {
            System.out.println("rowKey=" + Bytes.toString(result.getRow()));
            System.out.println("test_list=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("serial_num"))));
        }
    }

    /**
     * 扫描表
     */
    @Test
    public void ScanFileDetails() {
        ResultScanner scanner = HBaseUtil.getScanner("test_list", "rowkey1", "rowkey11");
        if (scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowKey=" + Bytes.toString(result.getRow()));
                System.out.println("test_list=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("serial_num"))));
            });
            scanner.close();
        }
    }

    /**
     * 删除rowkey数据
     */
    @Test
    public void deleteRow() {
        HBaseUtil.deleteRow("test_list", "rowkey11");
    }

    /**
     * 删除表
     */
    @Test
    public void deleteTable() {
        HBaseUtil.deleteTable("test_list");

        System.out.println("删除成功");
    }
}