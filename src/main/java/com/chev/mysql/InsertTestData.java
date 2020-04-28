package com.chev.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertTestData {

    private static final String insertQuery = "insert into voucher_barcodes (code, partition_id) values (?, ?)";

    /**
     Database name: test
     set autocommit=0;
     select @@autocommit;
     set global innodb_status_output=on;
     set global innodb_status_output_locks=on;
     create table voucher_barcodes (
     id bigint(20) unsigned not null auto_increment,
     code varchar(255) character set utf8 collate utf8_bin default null,
     partition_id int(11) not null,
     status enum('available', 'unavailable', 'returned') not null default 'available',
     primary key(id) key_block_size=8,
     key index_voucher_barcodes_on_partition_id_status(partition_id, status)
     )engine=innodb auto_increment=2180376288 default charset=utf8 row_format=dynamic;

     */

    public static void main(String[] args) {
        int totalRecordCount = 1000000;
        int batchCount = 1000;
        int totalBacthCount = totalRecordCount/batchCount;
        for (int i = 0; i < totalBacthCount; i++) {
            insertVoucherBarcodes(batchCount, batchCount * i, 1);
        }
    }

    private static void insertVoucherBarcodes(final int recordCount, final int startCodeId, final int partitionId) {
        try(final Connection connection = ConnectionPool.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (int i = 0; i < recordCount; i++) {
                    preparedStatement.setString(1, "c" + (startCodeId + i));
                    preparedStatement.setInt(2, partitionId);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
