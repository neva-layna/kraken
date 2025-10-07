package com.github.nlayna.kraken.executor.utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CsvFeeder {
    private final List<Map<String, String>> rows;
    private final AtomicInteger idx = new AtomicInteger(0);

    public CsvFeeder(List<Map<String, String>> rows) {
        this.rows = rows == null ? List.of() : rows;
    }

    public Map<String, String> next() {
        if (rows.isEmpty()) return Map.of();
        int i = Math.floorMod(idx.getAndIncrement(), rows.size());
        return rows.get(i);
    }
}
