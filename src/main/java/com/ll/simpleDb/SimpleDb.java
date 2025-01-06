package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDb {

    private Connection connection;
    public String DB_URL = "jdbc:mysql://host:3306/database?useSSL=false&allowPublicKeyRetrieval=devMode";
    public String user;
    public String password;

    public SimpleDb(String host, String user, String password, String database) {
        setDB_URL(host, database);
        this.user = user;
        this.password = password;
    }

    public void setDB_URL(String host, String database) {
        DB_URL = DB_URL.replace("host", host);
        DB_URL = DB_URL.replace("database", database);
    }

    public int run(String sql, Object... args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                if (args[i] instanceof Boolean) {
                    stmt.setBoolean(i + 1, (Boolean)args[i]);
                } else {
                    stmt.setString(i + 1, String.valueOf(args[i]));
                }
            }

            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> selectOne(String sql) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return toMap(resultSet);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public LocalDateTime selectDatetime(String sql) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getObject(1, LocalDateTime.class);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Boolean selectBoolean(String sql) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            ResultSet resultSet = stmt.executeQuery();

            if (resultSet.next()) {
                return resultSet.getObject(1, Boolean.class);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> select(String sql) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            ResultSet resultSet = stmt.executeQuery();

            return toMaps(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDevMode(boolean devMode) {
        DB_URL = DB_URL.replace("devMode", String.valueOf(devMode));
    }

    private ThreadLocal<Connection> threadLocalConnection = ThreadLocal.withInitial(() -> {
        try {
            return DriverManager.getConnection(DB_URL, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    public Connection getConnection() {
        return threadLocalConnection.get();
    }

    public void close() {
        try {
            if (getConnection() != null && !getConnection().isClosed()) {
                getConnection().close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void startTransaction() {
        try {
            getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            getConnection().rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            getConnection().commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }

    private Map<String, Object> toMap(ResultSet resultSet) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object columnValue = resultSet.getObject(i);
            row.put(columnName, columnValue);
        }

        return row;
    }

    private List<Map<String, Object>> toMaps(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object columnValue = resultSet.getObject(i);
                row.put(columnName, columnValue);
            }
            rows.add(row);
        }

        return rows;
    }
}
