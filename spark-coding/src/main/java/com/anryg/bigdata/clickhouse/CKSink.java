package com.anryg.bigdata.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @DESC: 自定义structured streaming的外部sink，通过jdbc写数据到clickhouse中
 * @Auther: Anryg
 * @Date: 2023/7/3 20:24
 */
public class CKSink extends ForeachWriter<Row> {
    private static final String jdbcUrl = "jdbc:ch://192.168.211.107:8123,192.168.211.108:8123,192.168.211.109:8123/local_db"; //为了防止找不到本地表，把整个集群的配置都写上
    //private static final Properties properties = new Properties();
    private static volatile ClickHouseDataSource ckDtaSource;
    private static volatile ClickHouseConnection connection;

    private static final String user = "default"; //用CK的默认用户
    private static final String pwd = "";     //默认用户没有设置密码
    private static final String tableName = "dns_logs_from_spark"; //写入的CK目标表


    /**
     * @DESC: 执行数据处理之前的准备工作，创建数据库连接，并确保单例，其中open会以partition为单位执行
     * */
    @Override
    public boolean open(long partitionId, long epochId){
        if (ckDtaSource == null || connection == null) {
            synchronized (CKSink.class){
                if (ckDtaSource == null || connection == null) {
                    try {
                        ckDtaSource = new ClickHouseDataSource(jdbcUrl);
                        connection = ckDtaSource.getConnection(user, pwd);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.exit(-1); //捕获到异常后进程退出
                    }
                }
            }
        }

        if (connection == null) return false;
        else return true;
    }


    /**
     * @DESC: 当open函数返回为true之后，会针对partition中的每个ROW进行调用
     * */
    @Override
    public void process(Row value) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("insert into " + tableName + " values(?,?,?,?,?,?,?,?,?)");
            preparedStatement.setString(1,value.getString(0));
            preparedStatement.setString(2,value.getString(1));
            preparedStatement.setString(3,value.getString(2));
            preparedStatement.setString(4,value.getString(3));
            preparedStatement.setString(5,value.getString(4));
            preparedStatement.setString(6,value.getString(5));
            preparedStatement.setString(7,value.getString(6));
            preparedStatement.setString(8,value.getString(7));
            preparedStatement.setString(9,value.getString(8));
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1); //捕获到异常后进程退出
        }

    }

    /**
     * @DESC: 上两个函数执行完后，开始调用，一般用于关闭连接
     * */
    @Override
    public void close(Throwable errorOrNull) {
        //长连接，不关闭
    }
}
