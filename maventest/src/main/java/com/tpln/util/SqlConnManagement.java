package com.tpln.util;

import java.sql.*;

/**
 * @author Shuai
 * @create 2021-01-15 10:28
 */
public class SqlConnManagement {

    String url = "";
    String user = "";
    String passwd = "";
    Connection conn = null;
    Statement stmt = null;

    public SqlConnManagement(String url, String user, String passwd) {
        this.url = url;
        this.user = user;
        this.passwd = passwd;
    }

    public Statement conn() throws SQLException {
        conn = DriverManager.getConnection(url, user,passwd);
        stmt = conn.createStatement();
        return stmt;
    }

    public void close() throws SQLException {
        if(stmt!=null) stmt.close();
        if(conn!=null) conn.close();
    }

}
