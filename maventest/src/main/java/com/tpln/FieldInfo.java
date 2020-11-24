package com.tpln;

import java.util.Objects;

/**
 * @author Shuai
 * @create 2020-10-09 18:36
 */
public class FieldInfo {
    private String fieldName = null;
    private String dataType = null;
    private String columnComment = null;


    public FieldInfo() {
    }

    public FieldInfo(String fieldName, String dataType, String columnComment) {
        this.fieldName = fieldName;
        this.dataType = dataType;
        this.columnComment = columnComment;
    }

    @Override
    public String toString() {
        return "FieldInfo{" +
                "fieldName='" + fieldName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", columnComment='" + columnComment + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInfo fieldInfo = (FieldInfo) o;
        return Objects.equals(fieldName, fieldInfo.fieldName) &&
                Objects.equals(dataType, fieldInfo.dataType) &&
                Objects.equals(columnComment, fieldInfo.columnComment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, dataType, columnComment);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

}
