package org.example;

import java.sql.*;
import java.util.Random;
import java.util.function.Consumer;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SpeedComparisonLite {

    private static final String DB_URL = "jdbc:sqlite:speed.db";
    private static final int NUM_ROWS = 10_000_000;


    /**
     * docker run --name my-postgres-db -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mypassword -e POSTGRES_DB=mydatabase -p 5432:5432 -d postgres
     * @param args
     */
    public static void main(String[] args) {

        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            if (conn != null) {
                createTables(conn);

                // normal
                long duration_normal = timeMethod(conn, SpeedComparisonLite::insertRows);

                // batches
                long duration_batch = timeMethod(conn, SpeedComparisonLite::insertRowsBatches);

                // prepared batches
                long duration_prepared = timeMethod(conn, SpeedComparisonLite::insertRowsPrepared);

                System.out.println("Total insertion time sequential: " + duration_normal / 1000.0 + " seconds");
                System.out.println("Total insertion time batch: " + duration_batch / 1000.0 + " seconds");
                System.out.println("Total insertion time prepared batch: " + duration_prepared / 1000.0 + " seconds");
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
        String tableNormal = "CREATE TABLE IF NOT EXISTS sample_data (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "value REAL NOT NULL)";

        String tablePrepared = "CREATE TABLE IF NOT EXISTS sample_data_prepared (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "value REAL NOT NULL)";

        Statement stmt = conn.createStatement();

        stmt.execute(tablePrepared);
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

                String insertSQL = String.format("INSERT INTO sample_data (name, value) VALUES ('Name_%d', %f)", i, random.nextDouble() * 1000);
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

                String insertSQL = String.format("INSERT INTO sample_data (name, value) VALUES ('Name_%d', %f)", i, random.nextDouble() * 1000);
                stmt.addBatch(insertSQL);
            }
            stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            System.out.println("Error during insertion: " + e.getMessage());
        }
    }

    /**
     * We prepare a single prepared insert statement which we enrich with all our rows by
     * attaching them as new batches to the statement.
     */
    private static void insertRowsPrepared(Connection conn) {
        String insertSQL = "INSERT INTO sample_data_prepared (name, value) VALUES (?, ?)";
        Random random = new Random();

        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            conn.setAutoCommit(false);

            for (int i = 1; i <= NUM_ROWS; i++) {
                pstmt.setString(1, "Name_" + i);
                pstmt.setDouble(2, random.nextDouble() * 1000);
                pstmt.addBatch();
            }


            pstmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            System.out.println("Error during batch insert: " + e.getMessage());
        }finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Error during batch insert: " + e.getMessage());
            }
        }
    }
}
