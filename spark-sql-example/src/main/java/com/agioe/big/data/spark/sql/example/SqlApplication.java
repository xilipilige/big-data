package com.agioe.big.data.spark.sql.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SqlApplication {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //循环执行runSparkPostgresqlJdbc
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            runSparkPostgresqlJdbc(sparkSession);
            System.out.println("runSparkPostgresqlJdbc耗时:"+(System.currentTimeMillis()-start));
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        sparkSession.stop();

    }

    private static void runSparkPostgresqlJdbc(SparkSession sparkSession) {
        long start = System.currentTimeMillis();
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "postgres");
        connectionProperties.put("driver", "org.postgresql.Driver");
        //从postgresql数据库中的alarm_real_time_event表读取数据
        Dataset<Row> jdbcDF = sparkSession.read()
                .jdbc("jdbc:postgresql://192.168.54.250:9701/datawarehouse_ya",
                        "alarm_real_time_event",
                        connectionProperties)
                .select("event_code", "event_level","event_subcategory_code","equipment_type_code","event_nature",
                "event_classify_code");
        System.out.println("加载数据耗时:"+(System.currentTimeMillis()-start));
        //分组统计

        long ananisy_start = System.currentTimeMillis();
        jdbcDF.groupBy("event_subcategory_code","equipment_type_code").count().show();
        System.out.println("分析数据耗时:"+(System.currentTimeMillis()-ananisy_start));
    }
}
