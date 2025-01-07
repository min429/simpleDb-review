package com.ll.simpleDb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDb {

    private String DB_URL = "jdbc:mysql://host:3306/database?useSSL=false&allowPublicKeyRetrieval=true";
    private String user;
    private String password;
    private boolean devMode;
    private ThreadLocal<Connection> threadLocalConnection = ThreadLocal.withInitial(() -> {
        try {
            return DriverManager.getConnection(DB_URL, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    public SimpleDb(String host, String user, String password, String database) {
        setDB_URL(host, database);
        this.user = user;
        this.password = password;
    }

    public void setDB_URL(String host, String database) {
        DB_URL = DB_URL.replace("host", host);
        DB_URL = DB_URL.replace("database", database);
    }

    public void setDevMode(boolean devMode) {
        this.devMode = devMode;
    }

    public void setAutoCommit(boolean autoCommit) {
        try {
            getConnection().setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String sql, Object... args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, List.of(args));
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long insert(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            parameterBinding(stmt, args);
            stmt.executeUpdate();
            try (ResultSet resultSet = stmt.getGeneratedKeys()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int modify(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, args);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> selectOne(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, args);
            try(ResultSet resultSet = stmt.executeQuery()){
                resultSet.next();
                return toMap(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public LocalDateTime selectDatetime(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, args);
            try(ResultSet resultSet = stmt.executeQuery()){
                resultSet.next();
                return resultSet.getObject(1, LocalDateTime.class);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Boolean selectBoolean(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, args);
            try(ResultSet resultSet = stmt.executeQuery()){
                resultSet.next();
                return resultSet.getObject(1, Boolean.class);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> select(String sql, List<Object> args) {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            parameterBinding(stmt, args);
            try(ResultSet resultSet = stmt.executeQuery()){
                return toMaps(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void parameterBinding(PreparedStatement stmt, List<Object> args) throws SQLException {
        for (int i = 0; i < args.size(); i++) {
            stmt.setObject(i + 1, args.get(i));
        }
    }

    public Connection getConnection() {
        try {
            Connection connection = threadLocalConnection.get();
            if (connection.isClosed()) {
                connection = DriverManager.getConnection(DB_URL, user, password);
                threadLocalConnection.set(connection);
            }
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
        setAutoCommit(false);
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
