package com.shuai;

import java.util.LinkedList;
import java.util.Objects;

/**
 * @author Shuai
 * @create 2020-10-09 16:15
 */
public class TableInfo {
    private String databaseName = null;
    private String tableName = null;
    private String tableComment = null;
    private LinkedList<FieldInfo> tableFields;

    public TableInfo() {
    }

    public TableInfo(String databaseName, String tableName, String tableComment) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableComment = tableComment;
    }

    public TableInfo(String databaseName, String tableName, String tableComment, LinkedList<FieldInfo> tableFields) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableComment = tableComment;
        this.tableFields = tableFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableInfo tableInfo = (TableInfo) o;
        return Objects.equals(databaseName, tableInfo.databaseName) &&
                Objects.equals(tableName, tableInfo.tableName) &&
                Objects.equals(tableComment, tableInfo.tableComment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tableComment);
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableComment='" + tableComment + '\'' +
                ", tableFields=" + tableFields +
                '}';
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public LinkedList<FieldInfo> getTableFields() {
        return tableFields;
    }

    public void setTableFields(LinkedList<FieldInfo> tableFields) {
        this.tableFields = tableFields;
    }
}
