package com.chev.mysql;

import java.sql.*;

public class App {
    public static void main( String[] args ) {
        int retry = 20;
        while (retry-- > 0) {
            System.out.println(retry);
        }
    }

    private static String insertStatement = "insert into voucher_barcodes (code) values (?)";

    private static void insertVoucherBarcodes() {
        try (final Connection connection = ConnectionPool.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertStatement)) {
                preparedStatement.setString(1, "code" + System.currentTimeMillis());
                final boolean execute = preparedStatement.execute();
                System.out.println(execute);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
