package org.example;

import java.sql.*;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SqlInjection {
    private static final String DB_URL = "jdbc:sqlite:sample.db";

    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(String[] args) {

        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            System.out.println("Connected to SQLite database.");

            createTable(connection);

            insertWithStatement(connection);

            insertData(connection, "Klaus");

            callInjection(connection);

            //queryPrepared(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS employeesInject (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                position TEXT NOT NULL,
                salary REAL
            );
        """;
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("Table 'employees' created.");
        }
    }

    private static void insertWithStatement(Connection connection) throws SQLException {
        String insertSQL1 = """
            INSERT INTO employeesInject (name, position, salary)
            VALUES ('Anna', 'Developer', 25000);
        """;

        String insertSQL2 = """
            INSERT INTO employeesInject (name, position, salary)
            VALUES ('Klaus', 'Manager', 75000);
        """;

        try (Statement statement = connection.createStatement()) {
            int rowsInserted = statement.executeUpdate(insertSQL1);
            rowsInserted += statement.executeUpdate(insertSQL2);
            System.out.println("Inserted " + rowsInserted + " row(s) with Statement.");
        }
    }

    private static void queryPrepared(Connection connection) throws SQLException {
        String sql = """
            SELECT * FROM employeesInject WHERE name = ?;
        """;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            ResultSet resultSet = pstmt.executeQuery();
            System.out.println("Result " + resultSet);
        }
    }

    private static void callInjection(Connection connection) throws SQLException {
        insertData(connection,"'Klaus'; INSERT INTO employeesInject (name, position, salary) VALUES ('Anna', 'Developer', 25000)");
    }

    private static void insertData(Connection connection, String name) throws SQLException {
        String querySQL = "INSERT INTO employeesInject (name, position, salary) VALUES ( " + name + ", 'Guest', 0)";

        Statement statement = connection.createStatement();
        boolean insert = statement.execute(querySQL);

        System.out.println("Inserted " + insert + ":" + insert);

    }
}
