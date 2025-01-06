package com.ll.simpleDb;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Sql {
    private StringBuilder stmt;
    private SimpleDb simpleDb;
    private ObjectMapper objectMapper;

    public Sql(SimpleDb simpleDb) {
        stmt = new StringBuilder();
        this.simpleDb = simpleDb;
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public Sql append(String sql) {
        stmt.append(" ").append(sql);
        return this;
    }

    public Sql append(String sql, Object... args) {
        stmt.append(" ").append(sql);

        Arrays.stream(args).forEach(
            arg -> {
                int idx = stmt.indexOf("?");
                if (arg instanceof String) {
                    arg = "'" + arg + "'";
                }
                stmt.replace(idx, idx + 1, String.valueOf(arg));
            }
        );

        return this;
    }

    public Sql appendIn(String sql, Object... args) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof String) {
                args[i] = "'" + args[i] + "'";
            }
            sb.append(args[i]);

            if (i < args.length - 1)
                sb.append(", ");
        }

        sql = sql.replace("?", sb.toString());

        stmt.append(" ").append(sql);

        return this;
    }

    public int insert() {
        return simpleDb.run(stmt.toString());
    }

    public int delete() {
        return simpleDb.run(stmt.toString());
    }

    public int update() {
        return simpleDb.run(stmt.toString());
    }

    public Map<String, Object> selectRow() {
        return simpleDb.selectOne(stmt.toString());
    }

    public <T> T selectRow(Class<T> type) {
        Map<String, Object> map = simpleDb.selectOne(stmt.toString());
        return objectMapper.convertValue(simpleDb.selectOne(stmt.toString()), type);
    }

    public List<Map<String, Object>> selectRows() {
        return simpleDb.select(stmt.toString());
    }

    public <T> List<T> selectRows(Class<T> type) {
        List<Map<String, Object>> maps = simpleDb.select(stmt.toString());

        return maps.stream().map(map -> objectMapper.convertValue(map, type)).collect(Collectors.toList());
    }

    public LocalDateTime selectDatetime() {
        return simpleDb.selectDatetime(stmt.toString());
    }

    public Long selectLong() {
        String column = getColumn();
        return (Long)simpleDb.selectOne(stmt.toString()).get(column);
    }

    public String selectString() {
        String column = getColumn();
        return (String)simpleDb.selectOne(stmt.toString()).get(column);
    }

    public Boolean selectBoolean() {
        System.out.println(stmt.toString());
        return simpleDb.selectBoolean(stmt.toString());
    }

    public List<Long> selectLongs() {
        String column = getColumn();

        List<Map<String, Object>> list = simpleDb.select(stmt.toString());

        return list.stream().map(map -> (Long)map.get(column)).collect(Collectors.toList());
    }

    private String getColumn() {
        int startIdx = stmt.indexOf("SELECT") + 7;
        int endIdx = stmt.indexOf("FROM") - 2;

        String column = stmt.substring(startIdx, endIdx + 1);
        return column;
    }
}
