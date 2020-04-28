package com.chev.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {

    private static ComboPooledDataSource comboPooledDataSource;

    public static void init() {
        comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl("jdbc:mysql://localhost:3306/test");
        comboPooledDataSource.setUser("root");
        comboPooledDataSource.setPassword("root");
        comboPooledDataSource.setMinPoolSize(10);
        comboPooledDataSource.setAcquireIncrement(5);
        comboPooledDataSource.setMaxPoolSize(20);
    }

    public static Connection getConnection() {
        try {
            Connection connection = comboPooledDataSource.getConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(4);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException("error while getting connection", e);
        }
    }
}
