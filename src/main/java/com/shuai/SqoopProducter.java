package com.shuai;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;


class FieldInfo {
    String fieldName;
    String dataType;
    String comnent;

    @Override
    public String toString() {
        return "FieldInfo{" +
                "fieldName='" + fieldName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", comnent='" + comnent + '\'' +
                '}';
    }
}

public class SqoopProducter {

//    public static final String URL = "jdbc:mysql://172.28.30.28:3306/test";
//    public static final String USER = "zhengyajun";
//    public static final String PASSWORD = "zhengyajun8899";

    public static final String URL = "jdbc:mysql://hadoop102:3306/gmall";
    public static final String USER = "root";
    public static final String PASSWORD = "123456";

    public static String sql = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS cmt \n" +
            "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
            "WHERE  a.TABLE_SCHEMA LIKE 'gmall' ;";

    private static LinkedHashMap<String, LinkedList<FieldInfo>> extractTabInfo() throws SQLException {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        //2. 获得数据库连接
        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
        //3.操作数据库，实现增删改查
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        //如果有数据，rs.next()返回true
        String previousTable = null;
        LinkedHashMap<String, LinkedList<FieldInfo>> allTabInfo = new LinkedHashMap<String, LinkedList<FieldInfo>>();
        LinkedList<FieldInfo> fields = null;
        String dbName = null;
        String tabName = null;
        String cluName = null;
        String cluType = null;
        String comment = null;
        String dbAndTable = null;
        while (rs.next()) {
            dbName = rs.getString("dbName");
            tabName = rs.getString("tabName");
            cluName = rs.getString("cluName");
            cluType = rs.getString("cluType");
            comment = rs.getString("cmt");
            dbAndTable = dbName + "." + tabName;

            if (previousTable == null || !previousTable.equals(dbAndTable)) {
                if (previousTable != null) {
                    allTabInfo.put(previousTable, fields);
                    fields = new LinkedList<FieldInfo>();
                }
                fields = new LinkedList<FieldInfo>();
                previousTable = dbAndTable;
            }

            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.fieldName = cluName;
            fieldInfo.dataType = cluType;
            fieldInfo.comnent = comment;
            fields.add(fieldInfo);


        }
        allTabInfo.put(dbAndTable, fields);
        return allTabInfo;
    }


    static void sqoopProducterFun(LinkedHashMap<String, LinkedList<FieldInfo>> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("sqoopTest.sh"));
        // write your code here

        out.write("#!/bin/bash\n\n" +
                "if [ -n \"$1\" ] ;then\n" +
                "    do_date=$1\n" +
                "else\n" +
                "    do_date=`date -d '-1 day' +%F`\n" +
                "fi\n\n");

        for (Map.Entry<String, LinkedList<FieldInfo>> entry : allTabInfo.entrySet()) {
            String key = entry.getKey();
            LinkedList<FieldInfo> value = entry.getValue();
            String[] split = key.split("\\.");

            //user permied？？
            StringBuilder sqoopTextBuilder = new StringBuilder();
            sqoopTextBuilder.append(
                    "hive -e 'create database if not exists " + split[0] + "'; \n" +
                            "sqoop import \\\n" +
                            "--connect jdbc:mysql://172.28.30.28:3306/" + split[0] + "?tinyInt1isBit=false \\\n" +
                            "--username zhengyajun \\\n" +
                            "--password zhengyajun8899 \\\n" +
                            "--table " + split[1] + " \\\n" +
                            "--target-dir /user/test4/db/" + split[0] + "/" + split[1] + "/" + "$1" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--num-mappers 2 \\\n" +
                            "--hive-import \\\n" +
                            "--hive-overwrite \\\n" +
                            "--hive-table " + split[0] + "." + split[1] + "  \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \\\n" +
                            "--hive-partition-key 'dt' \\\n" +
                            "--hive-partition-value $do_date ");

            boolean isFirstDecimal = true;
            for (FieldInfo fieldInfo : value) {
                String[] decimals = fieldInfo.dataType.split("\\(");
                if (decimals[0].equals("decimal")) {
                    if (isFirstDecimal) {
                        sqoopTextBuilder.append("\\\n");
                        sqoopTextBuilder.append("--map-column-hive ");
                        isFirstDecimal = false;
                    } else {
                        sqoopTextBuilder.append(",");
                    }

                    sqoopTextBuilder.append(fieldInfo.fieldName);
                    sqoopTextBuilder.append("=");
                    sqoopTextBuilder.append("\"DECIMAL(");
                    String encode = URLEncoder.encode(decimals[1].split("\\)")[0], "utf-8");
                    sqoopTextBuilder.append(encode);
                    sqoopTextBuilder.append(")\"");
                }
            }

            out.write(sqoopTextBuilder.toString());
            out.write("\n\n\n");
        }

        out.close();
    }


    static void sqoopProducterFun2(LinkedHashMap<String, LinkedList<FieldInfo>> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("sqoopTest.sh"));
        // write your code here

        out.write("#!/bin/bash\n\n" +
                "if [ -n \"$1\" ] ;then\n" +
                "    do_date=$1\n" +
                "else\n" +
                "    do_date=`date -d '-1 day' +%F`\n" +
                "fi\n\n");

        for (Map.Entry<String, LinkedList<FieldInfo>> entry : allTabInfo.entrySet()) {
            String key = entry.getKey();
            LinkedList<FieldInfo> value = entry.getValue();
            String[] split = key.split("\\.");

            //user permied？？
            StringBuilder sqoopTextBuilder = new StringBuilder();
            sqoopTextBuilder.append(
                    "hive -e 'create database if not exists " + split[0] + "'; \n" +
                            "drop table if exists " + split[0] + "; \n" +
                            "create external table " + split[0] + "(  \n");

            for (FieldInfo fieldInfo : value) {
                sqoopTextBuilder.append(
                        fieldInfo.fieldName + " " + fieldInfo.dataType + " COMMENT " + "\'" + fieldInfo.comnent+ "\' "
                );
                if (value.getLast() != fieldInfo) {
                    sqoopTextBuilder.append(" ,\n");
                }else {
                    sqoopTextBuilder.append("\n)\n");
                }
            }
            sqoopTextBuilder.append(
                    "PARTITIONED BY (`dt` string) \n"
                            + "location '/warehouse/gmall/ods/" + split[1] + "/';"
            );



            out.write(sqoopTextBuilder.toString());
            out.write("\n\n\n");
        }

        out.close();
    }


    public static void main(String[] args) throws IOException, SQLException {
//        BufferedWriter out = new BufferedWriter(new FileWriter("sqoopTest.sh"));
        // write your code here
//        String encode = URLEncoder.encode(("30,18"), "utf-8");
//        String decode = URLDecoder.decode(encode, "utf-8");
//        System.out.println(encode);
//        out.write(decode);
//
////        out.write("");
//        out.close();
//        System.out.println("文件创建成功！");
        //------------------------
        LinkedHashMap<String, LinkedList<FieldInfo>> allTabInfo = extractTabInfo();
        sqoopProducterFun2(allTabInfo);
    }
}
