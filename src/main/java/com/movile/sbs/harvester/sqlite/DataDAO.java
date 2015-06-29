package com.movile.sbs.harvester.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

import com.movile.sbs.harvester.bean.Record;

/**
 * @author eitikimura
 */
public class DataDAO {

    private Connection connection = null;
    private Statement statement;
    private PreparedStatement ps;

    public DataDAO(boolean recreate) {
        try {
            // load the sqlite-JDBC driver using the current class loader
            Class.forName("org.sqlite.JDBC");
            connection = null;

            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:data.db");
            statement = connection.createStatement();
            statement.setQueryTimeout(20); // set timeout to 180 sec.

            if (recreate) {
                statement.executeUpdate("DROP TABLE IF EXISTS raw_data");

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TEMP TABLE raw_data (");
                sb.append(" id INTEGER PRIMARY KEY AUTOINCREMENT,");
                sb.append(" key TEXT,");
                sb.append(" timestamp INTEGER,");
                sb.append(" type INTEGER,");
                sb.append(" priority INTEGER");
                //sb.append(" priority INTEGER,");
                //sb.append(" UNIQUE (key) ON CONFLICT REPLACE");
                sb.append(");");

                statement.executeUpdate(sb.toString());
            }

            ps = connection.prepareStatement("INSERT INTO raw_data (key, timestamp, type, priority) VALUES (?, ?, ?, ?)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getRowCount() throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) AS rowcount FROM raw_data");
        rs.next();
        int count = rs.getInt("rowcount");
        rs.close();
        return count;
    }

    public Statement getStatement() {
        return statement;
    }

    public void persist(List<Record> list) throws SQLException {

        list.stream().filter(Objects::nonNull).forEach(rec -> {
            try {
                ps.setString(1, rec.getKey());
                ps.setLong(2, rec.getTimestamp());
                ps.setInt(3, rec.getType());
                ps.setInt(4, rec.getPriority());
                ps.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        ps.executeBatch();
        ps.clearParameters();
    }

    public void close() throws SQLException {
        statement.close();
        connection.close();
    }

}
