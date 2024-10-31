package org.example;

import java.sql.*;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SQLiteExample {
    private static final String DB_URL = "jdbc:sqlite:sample.db";

    @SuppressWarnings("CallToPrintStackTrace")
    public static void main(String[] args) {

        try (Connection connection = DriverManager.getConnection(DB_URL)) {
            System.out.println("Connected to SQLite database.");

            createTable(connection);

            insertWithStatement(connection);

            insertWithBatch(connection);

            insertWithPreparedStatement(connection);

            queryData(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS employees (
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
        String insertSQL = """
            INSERT INTO employees (name, position, salary)
            VALUES ('Alice', 'Manager', 75000);
        """;
        try (Statement statement = connection.createStatement()) {
            int rowsInserted = statement.executeUpdate(insertSQL);
            System.out.println("Inserted " + rowsInserted + " row(s) with Statement.");
        }
    }

    private static void insertWithBatch(Connection connection) throws SQLException {
        String insertSQL = """
            INSERT INTO employees (name, position, salary) VALUES (?, ?, ?);
        """;
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {

            pstmt.setString(1, "Bob");
            pstmt.setString(2, "Developer");
            pstmt.setDouble(3, 60000);
            pstmt.addBatch();

            pstmt.setString(1, "Charlie");
            pstmt.setString(2, "Analyst");
            pstmt.setDouble(3, 55000);
            pstmt.addBatch();

            int[] rowsInserted = pstmt.executeBatch();
            System.out.println("Inserted " + rowsInserted.length + " row(s) with Batch.");
        }
    }

    private static void insertWithPreparedStatement(Connection connection) throws SQLException {
        String insertSQL = """
            INSERT INTO employees (name, position, salary) VALUES (?, ?, ?);
        """;
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, "David");
            pstmt.setString(2, "Designer");
            pstmt.setDouble(3, 50000);
            int rowsInserted = pstmt.executeUpdate();
            System.out.println("Inserted " + rowsInserted + " row(s) with PreparedStatement.");
        }
    }

    private static void queryData(Connection connection) throws SQLException {
        String querySQL = "SELECT * FROM employees;";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(querySQL)) {

            System.out.println("Employees data:");
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String position = resultSet.getString("position");
                double salary = resultSet.getDouble("salary");
                System.out.printf("ID: %d | Name: %s | Position: %s | Salary: %.2f%n", id, name, position, salary);
            }
        }
    }
}
