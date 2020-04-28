package com.chev.mysql;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Utils {

    private static final String selectForUpdateQuery = "select * from voucher_barcodes where partition_id = ? and status = ? limit 1 for update";

    private static final String updateByIdQuery = "update voucher_barcodes set status = ? where id = ?";

    private static final String selectByDbRandomPoolingQuery =
            "select *, rand(?) as random from (select * from voucher_barcodes where partition_id=? and status=? limit 50) temp order by random limit 1";

    private static final String selectByDbRandomPoolingQuery1 =
            "select * from voucher_barcodes where partition_id=? and status=? order by rand() limit 1";

    private static final String updateByIdStatusQuery = "update voucher_barcodes set status = ? where id = ? and status = ?";

    public static int consumeAvailableVoucherByDbRandomPooling(final int seed) {
        int flag = -1;

        Connection connection = null;
        PreparedStatement preparedStatement1 = null;
        ResultSet resultSet1 = null;
        PreparedStatement preparedStatement2 = null;
        long id = 0;

        try {
            connection = ConnectionPool.getConnection();

            preparedStatement1 = connection.prepareStatement(selectByDbRandomPoolingQuery);
//            preparedStatement1.setInt(1, 1);
//            preparedStatement1.setString(2, "available");

            preparedStatement1.setInt(1, seed);
            preparedStatement1.setInt(2, 1);
            preparedStatement1.setString(3, "available");
            resultSet1 = preparedStatement1.executeQuery();

            if (resultSet1.next()) {
                id = resultSet1.getLong("id");

                preparedStatement2 = connection.prepareStatement(updateByIdStatusQuery);
                preparedStatement2.setString(1, "unavailable");
                preparedStatement2.setLong(2, id);
                preparedStatement2.setString(3, "available");

                flag = preparedStatement2.executeUpdate();
            }

            connection.commit();
        } catch (SQLException e) {
            handleException(e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                handleException(e1);
            }
        } finally {
            try {
                if (null != resultSet1) resultSet1.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != preparedStatement1) preparedStatement1.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != preparedStatement2) preparedStatement2.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != connection) connection.close();
            } catch (SQLException e) {
                handleException(e);
            }
        }
        return flag;
    }

    private static void handleException(Exception e) {
        e.printStackTrace();
    }

    public static int consumeAvailableVoucherBySelectForUpdate () {
        int updatedRowCount = 0;

        Connection connection = null;
        PreparedStatement preparedStatement1 = null;
        ResultSet resultSet1 = null;
        PreparedStatement preparedStatement2 = null;
        long id = 0;

        try {
            connection = ConnectionPool.getConnection();

            preparedStatement1 = connection.prepareStatement(selectForUpdateQuery);
            preparedStatement1.setInt(1, 1);
            preparedStatement1.setString(2, "available");
            resultSet1 = preparedStatement1.executeQuery();
            if(resultSet1.next()) {
                id = resultSet1.getLong("id");

                preparedStatement2 = connection.prepareStatement(updateByIdQuery);
                preparedStatement2.setString(1, "unavailable");
                preparedStatement2.setLong(2, id);
                updatedRowCount = preparedStatement2.executeUpdate();
            }
            connection.commit();
        } catch (SQLException e) {
            handleException(e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                handleException(e1);
            }
        } finally {
            try {
                if (null != resultSet1) resultSet1.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != preparedStatement1) preparedStatement1.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != preparedStatement2) preparedStatement2.close();
            } catch (SQLException e) {
                handleException(e);
            }
            try {
                if (null != connection) connection.close();
            } catch (SQLException e) {
                handleException(e);
            }
        }
        return updatedRowCount;
    }

    public static void createLogDir(final String dirPath) throws IOException {
        final Path path = Paths.get(dirPath);
        if (Files.exists(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        new File(dirPath).mkdirs();
    }

    public static void main(String[] args) {
        ConnectionPool.init();
        int flag = -1;

        Connection connection = null;
        PreparedStatement preparedStatement1 = null;
        ResultSet resultSet1 = null;
        PreparedStatement preparedStatement2 = null;
        long id = 0;

        try {
            connection = ConnectionPool.getConnection();

            preparedStatement1 = connection.prepareStatement("select * from voucher_barcodes where partition_id = ? and status = ? limit 1");
            preparedStatement1.setInt(1, 1);
            preparedStatement1.setString(2, "available");
            resultSet1 = preparedStatement1.executeQuery();

            if (resultSet1.next()) {
                id = resultSet1.getLong("id");

                preparedStatement2 = connection.prepareStatement(updateByIdStatusQuery);
                preparedStatement2.setString(1, "unavailable");
                preparedStatement2.setLong(2, id);
                preparedStatement2.setString(3, "available");
                flag = preparedStatement2.executeUpdate();
            }

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (null != resultSet1) resultSet1.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (null != preparedStatement1) preparedStatement1.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (null != preparedStatement2) preparedStatement2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (null != connection) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println();
    }
}
