package com.chev.mysql;

import java.io.FileOutputStream;
import java.io.IOException;

public class SelectForUpdateTest {

    private static String logFilePath;

    private static final int threadCount = 20;

    private static final int recordsToUpdate = 10000;

    public static void main(String[] args) throws IOException {
        ConnectionPool.init();
        String path = "/home/chev/work/cheechu/mysql_concurrency/logs/selectforupdate/";
        path = path + "thread_count_" + threadCount + "_" + recordsToUpdate;
        Utils.createLogDir(path);
        logFilePath = path;

        for (int i = 0; i < threadCount; i++) {
            new Worker().start();
        }
    }

    private static final class Worker extends Thread {

        @Override
        public void run() {
            try (FileOutputStream fileOutputStream = new FileOutputStream(logFilePath + "/" + Thread.currentThread().getName())) {
                long startTime = 0;
                long time = 0;
                int updatedRowCount = 0;
                for (int i = 0; i < recordsToUpdate; i++) {
                    startTime = System.nanoTime();
                    updatedRowCount = Utils.consumeAvailableVoucherBySelectForUpdate();
                    time = System.nanoTime() - startTime;
                    fileOutputStream.write(String.valueOf(time).getBytes());
                    fileOutputStream.write(",".getBytes());
                    fileOutputStream.write(String.valueOf(updatedRowCount).getBytes());
                    fileOutputStream.write("\n".getBytes());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
