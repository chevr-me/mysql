package com.chev.mysql;

import java.io.*;
import java.util.*;

public class PerformanceTestDetails {

    public static void main(String[] args) throws IOException {
        //createReport("/home/chev/work/cheechu/mysql_concurrency/logs/selectforupdate/thread_count_20_10000");
        createReport("/home/chev/work/cheechu/mysql_concurrency/logs/dblevelrandompooling/thread_count_20_10000");
        //createReport("/home/chev/work/cheechu/mysql_concurrency/logs/hybriddblevelrandomselectforupdate/thread_count_20_10000");
    }

    private static void createReport(final String path) throws IOException {
        System.out.println("stats: " + path);
        addSpaces(13);
        System.out.print("executions");
        addSpaces(2);
        System.out.print("failed");
        addSpaces(6);
        System.out.print("p100");
        addSpaces(2);
        System.out.print("p99.9999");
        addSpaces(3);
        System.out.print("p99.999");
        addSpaces(4);
        System.out.print("p99.99");
        addSpaces(5);
        System.out.print("p99.9");
        addSpaces(7);
        System.out.print("p99");
        addSpaces(7);
        System.out.print("p95");
        addSpaces(7);
        System.out.print("p90");
        addSpaces(7);
        System.out.print("p50");
        addSpaces(7);
        System.out.print("avg");
        System.out.println();

        final File directory = new File(path);
        final File[] files = directory.listFiles();
        List<Long> allExecutionTimes = new ArrayList<>();
        long[] executions = new long[21];
        int totalFailedRows = 0;
        Map<Integer, Map<Integer, Integer>> executionTimeBucketRetriesRequestCountMap =
                new TreeMap<>(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer i1, Integer i2) {
                        return i2.compareTo(i1);
                    }
        });
        Map<Integer, Integer> retriesRequestCountMap;
        Integer requestCount;

        int executionTimeBucket = 0;

        List<Long> executionTimePerThread;
        BufferedReader bufferedReader;
        String row;
        String[] columns;
        long executionTime = 0;
        int failedRows = 0;
        int retries = 0;

        for (File file: files) {
            executionTimePerThread = new ArrayList<>();
            failedRows = 0;
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while(null != (row = bufferedReader.readLine())) {
                columns = row.split(",");
                executionTime = Long.parseLong(columns[0]);

                executionTimePerThread.add(executionTime);
                if("0".equals(columns[1])) {
                    failedRows++;
                }

                if(columns.length > 2) {
                    retries = Integer.parseInt(columns[2]);

                    executions[retries]++;

                    executionTime = executionTime/1000000;
                    executionTimeBucket = (int) (executionTime - (executionTime % 10));
                    retriesRequestCountMap = executionTimeBucketRetriesRequestCountMap.get(executionTimeBucket);
                    if(null == retriesRequestCountMap) {
                        retriesRequestCountMap = new HashMap<>();
                        executionTimeBucketRetriesRequestCountMap.put(executionTimeBucket, retriesRequestCountMap);
                    }
                    requestCount = retriesRequestCountMap.get(retries);
                    if(null == requestCount) {
                        requestCount = 0;
                    }
                    retriesRequestCountMap.put(retries, requestCount + 1);

                }
            }
            bufferedReader.close();

            totalFailedRows += failedRows;
            printStats(file.getName(), executionTimePerThread, failedRows);

            allExecutionTimes.addAll(executionTimePerThread);
        }

        printStats("agg stats", allExecutionTimes, totalFailedRows);

        System.out.print("executions stats: ");
        for (int i = 0; i < executions.length; i++) {
            System.out.print(i);
            System.out.print(":");
            System.out.print(executions[i]);
            System.out.print(',');
        }
        System.out.println();

        addSpaces(7);
        int retryCount = 21;
        for (int i = 1; i < retryCount; i++) {
            System.out.print(i);
            addSpaces(7 - String.valueOf(i).length());
        }
        System.out.println();

        final Set<Map.Entry<Integer, Map<Integer, Integer>>> executionTimeBucketRetriesRequestCountEntries =
                executionTimeBucketRetriesRequestCountMap.entrySet();
        for(Map.Entry<Integer, Map<Integer, Integer>> executionTimeBucketRetriesRequestCountEntry:
                executionTimeBucketRetriesRequestCountEntries) {
            System.out.print(executionTimeBucketRetriesRequestCountEntry.getKey());
            addSpaces(7 - String.valueOf(executionTimeBucketRetriesRequestCountEntry.getKey()).length());
            for (int i = 1; i < retryCount; i++) {
                requestCount = executionTimeBucketRetriesRequestCountEntry.getValue().get(i);
                if(null == requestCount) {
                    requestCount = 0;
                }
                System.out.print(requestCount);
                addSpaces(7 - String.valueOf(requestCount).length());
            }
            System.out.println();
        }
    }

    private static void printStats(String statsName, final List<Long> executionTimes, final int failedRows) {
        Collections.sort(executionTimes);

        long executionTimeSum = 0;
        for (Long executionTime: executionTimes) {
            executionTimeSum += executionTime;
        }

        System.out.print(statsName);
        addSpaces(9 - statsName.length());
        System.out.print(" -> ");
        addSpaces(10 - String.valueOf(executionTimes.size()).length());
        System.out.print(executionTimes.size());
        addSpaces(8 - String.valueOf(failedRows).length());
        System.out.print(failedRows);
        printStats(executionTimes, 100f);
        printStats(executionTimes, 99.9999f);
        printStats(executionTimes, 99.999f);
        printStats(executionTimes, 99.99f);
        printStats(executionTimes, 99.9f);
        printStats(executionTimes, 99f);
        printStats(executionTimes, 95f);
        printStats(executionTimes, 90f);
        printStats(executionTimes, 50f);

        executionTimeSum = executionTimeSum/executionTimes.size();
        executionTimeSum = executionTimeSum/1000000;
        addSpaces(10 - String.valueOf(executionTimeSum).length());
        System.out.println(executionTimeSum);
    }

    private static void printStats(final List<Long> executionTimes, final float percentile) {
        int index = (int) (percentile * executionTimes.size())/100;
        index--;
        String data = String.valueOf(executionTimes.get(index)/1000000);
        addSpaces(10 - data.length());
        System.out.print(data);
    }

    private static void addSpaces(int spaceCount) {
        for (int i = 0; i < spaceCount; i++) {
            System.out.print(" ");
        }
    }
}
