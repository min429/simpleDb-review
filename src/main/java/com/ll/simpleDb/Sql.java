package com.ll.simpleDb;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class Sql {
    private StringBuilder stmt;
    private SimpleDb simpleDb;
    private ObjectMapper objectMapper;
    private List<Object> params = new ArrayList<>();

    public Sql(SimpleDb simpleDb) {
        stmt = new StringBuilder();
        this.simpleDb = simpleDb;
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    public Sql append(String sql) {
        stmt.append(sql).append(" ");
        return this;
    }

    public Sql append(String sql, Object... args) {
        stmt.append(sql).append(" ");
        params.addAll(List.of(args));
        return this;
    }

    public Sql appendIn(String sql, Object... args) {
        List<String> marks = Stream.generate(() -> "?").limit(args.length).collect(Collectors.toList());
        String placeholders = String.join(", ", marks);

        sql = sql.replace("?", placeholders);

        stmt.append(sql).append(" ");
        params.addAll(List.of(args));

        return this;
    }

    public long insert() {
        return simpleDb.insert(stmt.toString(), params);
    }

    public int delete() {
        return simpleDb.modify(stmt.toString(), params);
    }

    public int update() {
        return simpleDb.modify(stmt.toString(), params);
    }

    public Map<String, Object> selectRow() {
        return simpleDb.selectOne(stmt.toString(), params);
    }

    public <T> T selectRow(Class<T> type) {
        return objectMapper.convertValue(simpleDb.selectOne(stmt.toString(), params), type);
    }

    public List<Map<String, Object>> selectRows() {
        return simpleDb.select(stmt.toString(), params);
    }

    public <T> List<T> selectRows(Class<T> type) {
        List<Map<String, Object>> maps = simpleDb.select(stmt.toString(), params);
        return maps.stream().map(map -> objectMapper.convertValue(map, type)).collect(Collectors.toList());
    }

    public LocalDateTime selectDatetime() {
        return simpleDb.selectDatetime(stmt.toString(), params);
    }

    public Long selectLong() {
        String column = getColumn();
        return (Long)simpleDb.selectOne(stmt.toString(), params).get(column);
    }

    public String selectString() {
        String column = getColumn();
        return (String)simpleDb.selectOne(stmt.toString(), params).get(column);
    }

    public Boolean selectBoolean() {
        return simpleDb.selectBoolean(stmt.toString(), params);
    }

    public List<Long> selectLongs() {
        String column = getColumn();
        List<Map<String, Object>> list = simpleDb.select(stmt.toString(), params);
        return list.stream().map(map -> (Long)map.get(column)).collect(Collectors.toList());
    }

    private String getColumn() {
        int startIdx = stmt.indexOf("SELECT") + 7;
        int endIdx = stmt.indexOf("FROM") - 2;

        String column = stmt.substring(startIdx, endIdx + 1);
        return column;
    }
}
