package com.agioe.big.data.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yshen
 * @since 19-2-26
 */
public class HBaseMonitorDataTest {

    @Test
    public void putData() {
        HBaseUtil.putRow("monitor_data", "100001-2019-2-27", "time", String.valueOf(System.currentTimeMillis()), "12.22");
    }

    /**
     * 检索数据
     */
    @Test
    public void getFileDetails() {
        Result result = HBaseUtil.getRow("monitor_data", "100001-2019-2-27");
        if (result != null) {
            System.out.println("rowKey=" + Bytes.toString(result.getRow()));

            for (Cell cell : result.listCells()) {
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    /**
     * 组合过滤检索
     */
    @Test
    public void filterListTest() throws IOException {
        for (int i = 0; i < 2; i++) {
            long start = System.currentTimeMillis();

            Table table = HBaseConn.getTable("monitor_data");
            List<Filter> filters = new ArrayList<Filter>();

            Filter rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("100001-2019-2-27")));


            Filter qualifierFilterLow = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1551230757875")));

            Filter qualifierFilterHigh = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1551230792404")));

            filters.add(rowKeyFilter);
            filters.add(qualifierFilterLow);
            filters.add(qualifierFilterHigh);

            FilterList filterList = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);

            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));

                for (Cell cell : r.listCells()) {
                    System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }

            }

            System.out.println("Hbase query time================：" + (System.currentTimeMillis() - start));
        }
    }
}
