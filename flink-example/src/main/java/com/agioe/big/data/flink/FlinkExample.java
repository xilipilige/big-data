package com.agioe.big.data.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author yshen
 * @since 19-3-4
 */
public class FlinkExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile("file:///root/flinkExampleFile");

        data.filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("https://");
            }
        }).writeAsText("file:///root/flinkExampleResultFile");

        JobExecutionResult res = env.execute();
    }
}
