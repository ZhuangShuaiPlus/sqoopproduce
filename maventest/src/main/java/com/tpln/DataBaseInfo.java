package com.tpln;

public class DataBaseInfo {
    private String url;
    private String user;
    private String password;
    private String sql;

    public DataBaseInfo() {
    }

    public DataBaseInfo(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public DataBaseInfo(String url, String user, String password, String sql) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.sql = sql;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return "DataBaseInfo{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
