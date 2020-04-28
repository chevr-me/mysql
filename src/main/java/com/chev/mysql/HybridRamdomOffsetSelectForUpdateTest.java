package com.chev.mysql;

public class HybridRamdomOffsetSelectForUpdateTest {

    private static final int maxOffset = 100;

    private static final String randomOffsetSelectQuery =
            "select * from voucher_barcodes where partition_id=? and status=? limit ?,1";

    private static final String updateQuery = "update voucher_barcodes set status = ? where id = ? and status = ?";
}
