package org.example;

import java.sql.*;
import java.util.Random;
import java.util.function.Consumer;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SpeedComparison {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/mydatabase";
    private static final String USER = "myuser";
    private static final String PASSWORD = "mypassword";

    private static final int NUM_ROWS = 100_000;


    /**
     * docker run --name my-postgres-db -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mypassword -e POSTGRES_DB=mydatabase -p 5432:5432 -d postgres
     */
    public static void main(String[] args) {

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            if (conn != null) {
                createTables(conn);

                // normal
                long duration_normal = timeMethod(conn, SpeedComparison::insertRows);

                // batches
                long duration_batch = timeMethod(conn, SpeedComparison::insertRowsBatches);

                System.out.println("Total insertion time sequential: " + duration_normal / 1000.0 + " seconds");
                System.out.println("Total insertion time batch: " + duration_batch / 1000.0 + " seconds");
            }
        } catch (SQLException e) {
            System.out.println("Error connecting to the database: " + e.getMessage());
        }
    }

    private static long timeMethod(Connection conn, Consumer<Connection> method) {
        long startTime = System.currentTimeMillis();
        method.accept(conn);
        long endTime = System.currentTimeMillis();

        return endTime - startTime;
    }

    private static void createTables(Connection conn) throws SQLException {
        String tableNormalDrop = "DROP TABLE IF EXISTS sample_data  ";
        String tableBatchDrop = "DROP TABLE IF EXISTS sample_data_batch";


        String tableNormal = "CREATE TABLE IF NOT EXISTS sample_data (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "value REAL NOT NULL)";

        String tableBatch = "CREATE TABLE IF NOT EXISTS sample_data_batch (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "value REAL NOT NULL)";


        Statement stmt = conn.createStatement();

        stmt.execute(tableNormalDrop);
        stmt.execute(tableBatchDrop);

        stmt.execute(tableBatch);
        stmt.execute(tableNormal);

    }

    /**
     * We execute the predefined amount of rows by constructing new SQL statements for each call
     * and sending them to the database.
     */
    private static void insertRows(Connection conn) {
        Random random = new Random();

        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            for (int i = 1; i <= NUM_ROWS; i++) {

                String insertSQL = String.format("INSERT INTO sample_data (id, name, value) VALUES (%d, 'Name_%d', %f)",i, i, random.nextDouble() * 1000);
                stmt.executeUpdate(insertSQL);
            }

            conn.commit();
        } catch (SQLException e) {
            System.out.println("Error during insertion: " + e.getMessage());
        }
    }

    /**
     * We execute the predefined amount of rows by constructing new SQL statements for each call, batching them
     * and sending them to the database.
     */
    private static void insertRowsBatches(Connection conn) {
        Random random = new Random();

        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            for (int i = 1; i <= NUM_ROWS; i++) {
                String insertSQL = String.format("INSERT INTO sample_data_batch (id, name, value) VALUES (%s, 'Name_%d', %f)", i, i, random.nextDouble() * 1000);
                stmt.addBatch(insertSQL);
            }
            stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            System.out.println("Error during insertion: " + e.getMessage());
        }
    }

}
