package com.agioe.big.data.hbase.es;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author yshen
 * @since 19-2-19
 */
public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HBaseConn() {
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "192.168.51.113");
        }
    }

    /**
     * 创建数据库连接
     *
     * @return
     */
    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取表实例
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

