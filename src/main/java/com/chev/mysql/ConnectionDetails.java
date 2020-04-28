package com.chev.mysql;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionDetails {
    public static void main(String[] args) {
        try (Connection connection = ConnectionPool.getConnection()) {
            System.out.println("auto_commit:" + connection.getAutoCommit());
            connection.setAutoCommit(false);
            System.out.println("auto_commit:" + connection.getAutoCommit());
            System.out.println("isolation_level:" + connection.getTransactionIsolation());
            System.out.println("readonly:" + connection.isReadOnly());
            System.out.println("closed:" + connection.isClosed());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
