package com.reiser.presto.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author: reiserx
 * Date:2021/9/28
 * Des:
 */
public class PrestoJDBC {
    public static void main(String[] args) {
        try {
            Connection conn = DriverManager.getConnection("jdbc:presto:///hive/default", "hadoop", null);
            Statement stmt = conn.createStatement();
            try {
                ResultSet rs = stmt.executeQuery("SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users FROM visit_summaries WHERE visit_date >= current_date - interval '7' day");
                while (rs.next()) {
                    long uv = rs.getLong(1);
                    System.out.printf(String.valueOf(uv));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                stmt.close();
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
