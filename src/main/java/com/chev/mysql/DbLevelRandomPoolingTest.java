package com.chev.mysql;

import java.io.FileOutputStream;
import java.io.IOException;

public class DbLevelRandomPoolingTest {

    private static String logFilePath;

    private static final int threadCount = 20;

    private static final int recordsToUpdate = 10000;

    public static void main(String[] args) throws IOException {
        ConnectionPool.init();
        String path = "/home/chev/work/cheechu/mysql_concurrency/logs/dblevelrandompooling/";
        path = path + "thread_count_" + threadCount + "_" + recordsToUpdate;
        Utils.createLogDir(path);
        logFilePath = path;

        for (int i = 0; i < threadCount; i++) {
            new Worker().start();
        }
    }

    private static final class Worker extends Thread {

        private int seed;

        @Override
        public void run() {
            seed = Thread.currentThread().getName().hashCode();

            try (FileOutputStream fileOutputStream = new FileOutputStream(logFilePath + "/" + Thread.currentThread().getName())) {
                long startTime = 0;
                long time = 0;
                int[] executionResult = null;
                for (int i = 0; i < recordsToUpdate; i++) {
                    startTime = System.nanoTime();
                    executionResult = getAvailableVoucherCodeWithRetry();
                    time = System.nanoTime() - startTime;
                    fileOutputStream.write(String.valueOf(time).getBytes());
                    fileOutputStream.write(",".getBytes());
                    fileOutputStream.write(String.valueOf(executionResult[0]).getBytes());
                    fileOutputStream.write(",".getBytes());
                    fileOutputStream.write(String.valueOf(executionResult[1]).getBytes());
                    fileOutputStream.write("\n".getBytes());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private int[] getAvailableVoucherCodeWithRetry() {
            int executions = 0;
            int availableVoucherCode = 0;
            int retry = 20;
            while (retry-- > 0) {
                executions++;
                availableVoucherCode = Utils.consumeAvailableVoucherByDbRandomPooling(seed);
                if (0 != availableVoucherCode) {
                    break;
                }
            }
            return new int[] {availableVoucherCode, executions};
        }
    }
}


