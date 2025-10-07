package com.github.nlayna.kraken.executor.records;

public record ResultRow(String isoTs, long startEpochMs, long latencyMs, int status, boolean success,
                        String error, String method, String path, int vu, long reqBytes, long respBytes) {
}
