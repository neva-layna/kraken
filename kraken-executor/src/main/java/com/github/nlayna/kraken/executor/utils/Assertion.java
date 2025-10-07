package com.github.nlayna.kraken.executor.utils;

import com.alibaba.fastjson2.JSONPath;
import com.alibaba.fastjson2.JSONReader;

import java.util.Objects;

public class Assertion {
    private final String op;       // == or != or > < >= <=
    private final String rhs;
    private final JSONPath jsonPath;

    public Assertion(String expr) {
        String e = expr.trim();
        if (e.startsWith(".")) {
            e = "$" + e;
        } // allow .statusCode syntax

        // find operator
        String[] ops = {"==", "!=", ">=", "<=", ">", "<"};
        int pos = -1; String found = null;

        for (String o : ops) {
            int i = e.indexOf(o);
            if (i>0) {
                pos = i; found=o;
                break;
            }
        }

        if (pos < 0) {
            throw new IllegalArgumentException("Bad assertion: " + expr);
        }

        this.jsonPath = JSONPath.of(e.substring(0, pos).trim());
        this.op = found;
        this.rhs = e.substring(pos + found.length()).trim();
    }

    public boolean evaluate(String json) {
        try (JSONReader parser = JSONReader.of(json)) {
            Object left = jsonPath.extract(parser);
            Object right = getRight();
            int cmp = compare(left, right);
            return switch (op) {
                case "==" -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">" -> cmp > 0;
                case "<" -> cmp < 0;
                case ">=" -> cmp >= 0;
                case "<=" -> cmp <= 0;
                default -> false;
            };
        }
    }

    private Object getRight() {
        Object right;
        if ((rhs.startsWith("\"") && rhs.endsWith("\"")) || (rhs.startsWith("'") && rhs.endsWith("'"))) {
            right = rhs.substring(1, rhs.length()-1);
        } else if ("true".equalsIgnoreCase(rhs) || "false".equalsIgnoreCase(rhs)) {
            right = Boolean.parseBoolean(rhs);
        } else {
            try {
                right = rhs.contains(".") ? Double.parseDouble(rhs) : Long.parseLong(rhs);
            } catch (NumberFormatException nfe) {
                right = rhs;
            }
        }
        return right;
    }

    private int compare(Object l, Object r) {
        if (Objects.equals(l, r)) {
            return 0;
        }
        if (l instanceof Number ln && r instanceof Number rn) {
            double a = ln.doubleValue(), b = rn.doubleValue();
            return Double.compare(a, b);
        }
        return String.valueOf(l).compareTo(String.valueOf(r));
    }

    @Override
    public String toString() { return jsonPath + op + rhs; }
}
