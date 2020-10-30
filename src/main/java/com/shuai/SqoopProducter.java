package com.shuai;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

//shuai
public class SqoopProducter {


//测试Tidb
//    public static final String URL = "jdbc:mysql://172.28.30.28:3306";
//    public static final String USER = "zhengyajun";
//    public static final String PASSWORD = "zhengyajun8899";

//自己电脑
//    public static final String URL = "jdbc:mysql://hadoop102:3306/gmall";
//    public static final String USER = "root";
//    public static final String PASSWORD = "123456";

    //生产Tidb
    public static final String URL = "jdbc:mysql://10.16.40.154:4000/";
    public static final String USER = "zhuangshuai";
    public static final String PASSWORD = "0UtLSBLnYajUutJh";

    //测试drds--kk
//    public static final String URL = "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/";
//    public static final String USER = "agent_rw";
//    public static final String PASSWORD = "ezuYvaJppO3kPmOgqqPY";

//测试drds--english
//    public static final String URL = "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/";
//    public static final String USER = "qa_alldata_ro";
//    public static final String PASSWORD = "kUM8qL1cubf0Xf7RI9AT";

    //    public static final String PREFIX = "ods";
    public static final String PREFIX = "eng";
    //    public static final String LAYERPREFIX = "ods_";
    public static final String LAYERPREFIX = "";
    public static final String ODSSHELLLOCATION = "/root/bin/azkabantest/hdfs_to_ods";
    public static final String DWDSHELLLOCATION = "/root/bin/azkabantest/ods_to_dwd";
    public static final String SQOOPSHELLLOCATION = "/root/bin/azkabantest/oltp_to_hdfs";

    public static final HashSet<String> INCREMENTTABLE = new HashSet<String>();


    public static String SQL = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
            "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
            "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
            "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
//            "WHERE  a.TABLE_SCHEMA LIKE '" + PREFIX + "%' ;";
//dwd层
//            "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' ;";
//            "WHERE  a.TABLE_SCHEMA = 'english_agent' ;";
            //kk的表
            "WHERE  a.TABLE_SCHEMA in (" +
            "ientrepreneurship_dev" + ");";


    static LinkedHashSet<TableInfo> extractAllTabInfo() throws SQLException {
        LinkedList<DataBaseInfo> datalist = new LinkedList<DataBaseInfo>();

        String sqleng = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "eng" + "%' ;";

        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";

        String sqlch = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chinese" + "%' ;";


        //英语测试
        datalist.add(new DataBaseInfo(
                "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/",
                "qa_alldata_ro",
                "kUM8qL1cubf0Xf7RI9AT",
                sqleng
        ));

        //KK测试
        datalist.add(new DataBaseInfo(
                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
                "agent_rw",
                "ezuYvaJppO3kPmOgqqPY",
                sqlkk
        ));

        //语文测试
        datalist.add(new DataBaseInfo(
                "jdbc:mysql://172.28.30.87:3306/",
                "qa_new_chinese",
                "Aa123456",
                sqlch
        ));


        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();

        for (DataBaseInfo dataBaseInfo : datalist) {
            LinkedHashSet<TableInfo> tableInfos = extractTabInfo2(dataBaseInfo, dataBaseInfo.getUrl());
            allTabInfo.addAll(tableInfos);
        }
        return allTabInfo;
    }


    static LinkedHashSet<TableInfo> extractTabInfo(String url, String user, String password, String sql) throws SQLException {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        TableInfo previousTable = null;
        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();
        LinkedList<FieldInfo> fields = null;
        String dbName = null;
        String tabName = null;
        String cluName = null;
        String cluType = null;
        String columnComment = null;
        String tblComment = null;
        TableInfo dbAndTable = null;
        while (rs.next()) {
            dbName = rs.getString("dbName");
            tabName = rs.getString("tabName");
            cluName = rs.getString("cluName");
            cluType = rs.getString("cluType");
            columnComment = rs.getString("columnComment");
            tblComment = rs.getString("tblComment");
            dbAndTable = new TableInfo(dbName, tabName, tblComment);


            if (previousTable == null || !previousTable.equals(dbAndTable)) {
                if (previousTable != null) {
                    previousTable.setTableFields(fields);
                    allTabInfo.add(previousTable);
                }
                fields = new LinkedList<FieldInfo>();
                previousTable = dbAndTable;
            }

            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.setFieldName(cluName);
            fieldInfo.setDataType(cluType);
            fieldInfo.setColumnComment(columnComment);
            fields.add(fieldInfo);
        }
        dbAndTable.setTableFields(fields);
        allTabInfo.add(dbAndTable);
        return allTabInfo;
    }


    static LinkedHashSet<TableInfo> extractTabInfo2(DataBaseInfo dataBaseInfo, String sql) throws SQLException {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        TableInfo previousTable = null;
        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();
        LinkedList<FieldInfo> fields = null;
        String dbName = null;
        String tabName = null;
        String cluName = null;
        String cluType = null;
        String columnComment = null;
        String tblComment = null;
        TableInfo dbAndTable = null;
        while (rs.next()) {
            dbName = rs.getString("dbName");
            tabName = rs.getString("tabName");
            cluName = rs.getString("cluName");
            cluType = rs.getString("cluType");
            columnComment = rs.getString("columnComment");
            tblComment = rs.getString("tblComment");
            dbAndTable = new TableInfo(dbName, tabName, tblComment);


            if (previousTable == null || !previousTable.equals(dbAndTable)) {
                if (previousTable != null) {
                    previousTable.setTableFields(fields);
                    allTabInfo.add(previousTable);
                }
                fields = new LinkedList<FieldInfo>();
                previousTable = dbAndTable;
            }

            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.setFieldName(cluName);
            fieldInfo.setDataType(cluType);
            fieldInfo.setColumnComment(columnComment);
            fields.add(fieldInfo);
        }
        dbAndTable.setTableFields(fields);
        allTabInfo.add(dbAndTable);
        return allTabInfo;
    }


    static void everyTableOdsShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./hdfs_to_ods/" + LAYERPREFIX + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./hdfs_to_ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
//            stringBuilder.append(
//                    "sqoop import \\\n" +
//                            "--connect " + URL + tableInfo.getDatabaseName() + " \\\n" +
//                            "--username " + USER + " \\\n" +
//                            "--password " + PASSWORD + " \\\n" +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
//                            "--target-dir /user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
//                            "--delete-target-dir \\\n" +
//                            "--fields-terminated-by '\\001' \\\n" +
//                            "--num-mappers 1 \\\n" +
//                            "--null-string '\\\\N' \\\n" +
//                            "--null-non-string '\\\\N' \n\n"
//            );
//            exceptionExitSqoop(stringBuilder, tableInfo);
//            stringBuilder.append("\n");
            stringBuilder.append("hive -e \"load data inpath '/bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    static void everyTableSqoopShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {


        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "--connect " + URL + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + USER + " \\\n" +
                            "--password " + PASSWORD + " \\\n" +
                            odsSyncStrategy(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 1 \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExitSqoop(stringBuilder, tableInfo);
//            stringBuilder.append("\n");
//            stringBuilder.append("hive -e \"load data inpath '/user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
            //azkaban报错用
//            exceptionExit(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    private static String odsSyncStrategy(TableInfo tableInfo) {
        if (!INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName())) {
            return "--table " + tableInfo.getTableName() + " \\\n";
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            LinkedList<FieldInfo> tableFields = tableInfo.getTableFields();
            stringBuilder.append("--query \"select\n");
            int size = tableFields.size();
            for (FieldInfo fieldInfo : tableFields) {
                size--;
                stringBuilder.append(fieldInfo.getFieldName());
                //比计数慢，还是快
//                if(tableFields.getLast().equals(fieldInfo)){
                if (size != 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\n");
            }
            stringBuilder.append("from " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n"
                    + "where date_format(createTime,'%Y-%m-%d') = \'$do_date\'\n"
                    + "and \\$CONDITIONS\" \\\n"
            );
            return stringBuilder.toString();
        }
    }

    //    static void sqoopProducterFun(LinkedHashMap<String, LinkedList<FieldInfo>> allTabInfo) throws IOException {
//
//        BufferedWriter out = new BufferedWriter(new FileWriter("sqoopTest.sh"));
//        // write your code here
//
//        out.write("#!/bin/bash\n\n" +
//                "if [ -n \"$1\" ] ;then\n" +
//                "    do_date=$1\n" +
//                "else\n" +
//                "    do_date=`date -d '-1 day' +%F`\n" +
//                "fi\n\n");
//
//        for (Map.Entry<String, LinkedList<FieldInfo>> entry : allTabInfo.entrySet()) {
//            String key = entry.getKey();
//            LinkedList<FieldInfo> value = entry.getValue();
//            String[] split = key.split("\\.");
//
//            //user permied？？
//            StringBuilder sqoopTextBuilder = new StringBuilder();
//            sqoopTextBuilder.append(
//                    "hive -e 'create database if not exists " + split[0] + "'; \n" +
//                            "sqoop import \\\n" +
//                            "--connect jdbc:mysql://172.28.30.28:3306/" + split[0] + "?tinyInt1isBit=false \\\n" +
//                            "--username zhengyajun \\\n" +
//                            "--password zhengyajun8899 \\\n" +
//                            "--table " + split[1] + " \\\n" +
//                            "--target-dir /user/test4/db/" + split[0] + "/" + split[1] + "/" + "$1" + " \\\n" +
//                            "--delete-target-dir \\\n" +
//                            "--num-mappers 2 \\\n" +
//                            "--hive-import \\\n" +
//                            "--hive-overwrite \\\n" +
//                            "--hive-table " + split[0] + "." + split[1] + "  \\\n" +
//                            "--null-string '\\\\N' \\\n" +
//                            "--null-non-string '\\\\N' \\\n" +
//                            "--hive-partition-key 'dt' \\\n" +
//                            "--hive-partition-value $do_date ");
//
//            boolean isFirstDecimal = true;
//            for (FieldInfo fieldInfo : value) {
//                String[] decimals = fieldInfo.dataType.split("\\(");
//                if (decimals[0].equals("decimal")) {
//                    if (isFirstDecimal) {
//                        sqoopTextBuilder.append("\\\n");
//                        sqoopTextBuilder.append("--map-column-hive ");
//                        isFirstDecimal = false;
//                    } else {
//                        sqoopTextBuilder.append(",");
//                    }
//
//                    sqoopTextBuilder.append(fieldInfo.fieldName);
//                    sqoopTextBuilder.append("=");
//                    sqoopTextBuilder.append("\"DECIMAL(");
//                    String encode = URLEncoder.encode(decimals[1].split("\\)")[0], "utf-8");
//                    sqoopTextBuilder.append(encode);
//                    sqoopTextBuilder.append(")\"");
//                }
//            }
//
//            out.write(sqoopTextBuilder.toString());
//            out.write("\n\n\n");
//        }
//
//        out.close();
//    }
    static void mysqlToHdfs(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("tidb_to_hdfs.sh"));

        out.write("#!/bin/bash\n\n" +
                "if [ -n \"$1\" ] ;then\n" +
                "    do_date=$1\n" +
                "else\n" +
                "    do_date=`date -d '-1 day' +%F`\n" +
                "fi\n\n");


        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
//            if (tableInfo.getDatabaseName() != "english_agent") {
//                continue;
//            }
            stringBuilder.append(
                    "sqoop import \\\n" +
//                            "--connect jdbc:mysql://172.28.30.28:3306/" + tableInfo.getDatabaseName() + " \\\n" +
                            "--connect " + URL + tableInfo.getDatabaseName() + " \\\n" +
//                            "--username zhengyajun \\\n" +
                            "--username " + USER + " \\\n" +
//                            "--password zhengyajun8899 \\\n" +
                            "--password " + PASSWORD + " \\\n" +
                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 1 \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            //azkaban报错用
            exceptionExitSqoop(stringBuilder, tableInfo);

        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }

    static void hiveCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table.sh"));

//        out.write("#!/bin/bash\n\n" +
//                "if [ -n \"$1\" ] ;then\n" +
//                "    do_date=$1\n" +
//                "else\n" +
//                "    do_date=`date -d '-1 day' +%F`\n" +
//                "fi\n\n");

        for (TableInfo tableInfo : allTabInfo) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(
                    "hive -e 'create database if not exists " + tableInfo.getDatabaseName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "drop table if exists " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "create external table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "(  \n");

            for (FieldInfo fieldInfo : tableInfo.getTableFields()) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                if (tableInfo.getTableFields().getLast() != fieldInfo) {
                    stringBuilder.append(" ,\n");
                } else {
                    stringBuilder.append("\n)\n");
                }
            }
            stringBuilder.append("COMMENT \"" + tableInfo.getTableComment() + "\"\n");
            stringBuilder.append(
                    "PARTITIONED BY (`dt` string) \n" +
                            "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/user/warehouse/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";';\n\n"
            );

            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }

        out.close();
    }


    static void hiveCreateTable2(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table.sh"));

        out.write("hive -e '");

        int i = 0;
        for (TableInfo tableInfo : allTabInfo) {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(
                    "create database if not exists " + LAYERPREFIX + tableInfo.getDatabaseName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "drop table if exists " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "create external table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "(  \n");

            for (FieldInfo fieldInfo : tableInfo.getTableFields()) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fliterQuotation(fieldInfo.getColumnComment()) + "\" "
                );
                if (tableInfo.getTableFields().getLast() != fieldInfo) {
                    stringBuilder.append(" ,\n");
                } else {
                    stringBuilder.append("\n)\n");
                }
            }
            stringBuilder.append("COMMENT \"" + tableInfo.getTableComment() + "\"\n");
            stringBuilder.append(
                    "PARTITIONED BY (`dt` string) \n" +
                            "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/bigdata/warehouse/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";\n"
            );

//            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
//            out.write("\n\n");
            if (i % 50 == 0) {
                out.write("';\n hive -e '\n");
            }
            i++;

        }
        out.write("';\n");

        out.close();
    }

    static String fliterQuotation(String s) throws IOException {
//        if (s.contains("\'") || s.contains("\"")) {
        s = s.replaceAll("\"", "");
        s = s.replaceAll("\'", "");
//            return s.replaceAll("\"", "");
//        }
        return s;
    }

    static void hiveCreateTable3(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table.sh"));

        for (TableInfo tableInfo : allTabInfo) {

            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("hive -e '");
            stringBuilder.append(
                    "create database if not exists " + tableInfo.getDatabaseName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "drop table if exists " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "create external table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "(  \n");

            for (FieldInfo fieldInfo : tableInfo.getTableFields()) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                if (tableInfo.getTableFields().getLast() != fieldInfo) {
                    stringBuilder.append(" ,\n");
                } else {
                    stringBuilder.append("\n)\n");
                }
            }
            stringBuilder.append("COMMENT \"" + tableInfo.getTableComment() + "\"\n");
            stringBuilder.append(
                    "PARTITIONED BY (`dt` string) \n" +
                            "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/bigdata/warehouse/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";';\n\n"
            );

            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }
        out.write("';\n");

        out.close();
    }

    static void dwdCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("dwd_create_table.sh"));
        for (TableInfo tableInfo : allTabInfo) {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("hive -e '");
            stringBuilder.append(
                    "create database if not exists " + tableInfo.getDatabaseName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "drop table if exists " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "create external table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "(  \n");

            boolean isPartitionTable = false;
            int size = tableInfo.getTableFields().size();
            LinkedList<FieldInfo> tableFields = tableInfo.getTableFields();
            for (int i = 0; i < size; i++) {
                FieldInfo fieldInfo = tableFields.get(i);
                if (i + 2 == size && "date_value".equals(tableFields.get(i + 1).getFieldName())) {
                    stringBuilder.append(
                            "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                    );
                    stringBuilder.append("\n)\n");
                    isPartitionTable = true;
                    break;
                }
                if ("date_value".equals(fieldInfo.getFieldName())) {
                    isPartitionTable = true;
                    continue;
                }
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                //最后一个不加逗号
                if (tableInfo.getTableFields().getLast() != fieldInfo) {
                    stringBuilder.append(" ,\n");
                } else {
                    stringBuilder.append("\n)\n");
                }
            }
            stringBuilder.append("COMMENT \"" + tableInfo.getTableComment() + "\"\n");
            //是分区表则加入分区
            if (isPartitionTable) {
                stringBuilder.append("PARTITIONED BY (`dt` string) \n");
            }
            stringBuilder.append(
//                    "PARTITIONED BY (`dt` string) \n" +
                    "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/bigdata/warehouse/dwd/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";");
            stringBuilder.append("';\n\n");
            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }
//        out.write("';\n");
        out.close();
    }


    static void hiveLoadData(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("hdfs_to_ods_db.sh"));

        out.write("#!/bin/bash\n\n" +
                "if [ -n \"$1\" ] ;then\n" +
                "    do_date=$1\n" +
                "else\n" +
                "    do_date=`date -d '-1 day' +%F`\n" +
                "fi\n\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("hive -e \"load data inpath '/user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n");
            exceptionExitLoadData(stringBuilder, tableInfo);
        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }

    static String fieldMap(String dataType) {
        String[] split = dataType.split("\\(");
        String hiveDataType = null;
        switch (split[0]) {
            case "date":
            case "datetime":
            case "timestamp":
                hiveDataType = "string";
                break;
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "float":
            case "double":
                hiveDataType = split[0];
                break;
            case "mediumint":
                hiveDataType = "int";
                break;
            case "varchar":
            case "text":
            case "mediumtext":
            case "longtext":
                hiveDataType = "string";
                break;
            case "bit":
                hiveDataType = "boolean";
                break;
            case "json":
                hiveDataType = "string";
                break;
            default:
                hiveDataType = dataType;
        }
        return hiveDataType;
    }

    //load data error
    static void exceptionExitLoadData(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \"Hive load data: error in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"Hive load data: successful in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }

    static void exceptionExitCreateTable(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \"Hive create table: error in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"Hive create table: successful in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }

    //sqoop error
    static void exceptionExitSqoop(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \"sqoop: error in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"sqoop: successful in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }


    static void azkabanOdsFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./azkaban-data-warehouse/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "DataWarehouse.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "DataWarehouse.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    config:\n" +
                    "      command: sh " + SQOOPSHELLLOCATION + "/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");


            stringBuilder.append("  - name: ods." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + ODSSHELLLOCATION + "/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

//        for (TableInfo tableInfo : allTabInfo) {
//            stringBuilder.append("  - name: sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
//                    "    type: command\n" +
//                    "    dependsOn:\n" +
//                    "      - " + "sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
//                    "    config:\n" +
//                    "      command: sh " + ODSSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
//        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }

    static void azkabanDwdFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./dwd-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "dwd_schedule.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "dwd_schedule.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {

            stringBuilder.append("  - name: dwd." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "ods." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + DWDSHELLLOCATION + "/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

//        for (TableInfo tableInfo : allTabInfo) {
//            stringBuilder.append("  - name: sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
//                    "    type: command\n" +
//                    "    dependsOn:\n" +
//                    "      - " + "sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
//                    "    config:\n" +
//                    "      command: sh " + ODSSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
//        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }


//    static void createAzkabanShellExecDir(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
//
//
//        BufferedWriter out = new BufferedWriter(new FileWriter("createAzkabanShellExecDir.sh"));
//
//        out.write("#!/bin/bash\n\n" +
//                "if [ -n \"$1\" ] ;then\n" +
//                "    do_date=$1\n" +
//                "else\n" +
//                "    do_date=`date -d '-1 day' +%F`\n" +
//                "fi\n\n");
//
//        StringBuilder stringBuilder = new StringBuilder();
//        for (TableInfo tableInfo : allTabInfo) {
//            stringBuilder.append("hive -e \"load data inpath '/user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n");
//            exceptionExit(stringBuilder, tableInfo);
//        }
//
//        out.write(stringBuilder.toString());
//        out.write("\n\n\n");
//        out.close();
//
//    }

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

//        System.out.println(fliterQuotation("'aaba':\"2232\"'"));
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo();
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo(URL, USER, PASSWORD, sql);

//        INCREMENTTABLE.add("ods_english_game.ods_gm_player_property_change");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_day_total_read");
//        INCREMENTTABLE.add("ods_chinese_power_value.ods_word_study_stat");
//        INCREMENTTABLE.add("ods_english_ebook.ods_ek_student_first_ebook");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_day_read");
//        INCREMENTTABLE.add("ods_english_video.ods_hs_speak_student_answer");
//        INCREMENTTABLE.add("ods_chinese_article.ods_article_sentence_info");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_through_book");
//        INCREMENTTABLE.add("ods_english_parent.ods_pt_heart_talk");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_student_interrupt");
//        INCREMENTTABLE.add("ods_english_gold.ods_gd_gold_bill");
//        INCREMENTTABLE.add("ods_english_game.ods_gm_player_task");
//        INCREMENTTABLE.add("ods_english_video.ods_vd_play_film_log");
//        INCREMENTTABLE.add("ods_english_oral.ods_ol_day_oral");
//        INCREMENTTABLE.add("ods_chinese_article.ods_read_article_info");
//        INCREMENTTABLE.add("ods_chinese_article.ods_read_article_log");
//        INCREMENTTABLE.add("ods_english_oral.ods_ol_day_oral_bak");
//        INCREMENTTABLE.add("ods_english_parent.ods_pt_read_activity_sign_in");
//        INCREMENTTABLE.add("ods_english_student.ods_st_student_view");
//        INCREMENTTABLE.add("ods_english_ebook.ods_ek_student_ebook");
//        INCREMENTTABLE.add("ods_english_oral.ods_ol_student_review");
//        INCREMENTTABLE.add("ods_english_student.ods_st_student_report");
//        INCREMENTTABLE.add("ods_english_oral.ods_ol_student_review_bak");
//        INCREMENTTABLE.add("ods_english_parent.ods_pt_upload_sign_pic");
//        INCREMENTTABLE.add("ods_chinese_power_value.ods_user_word_study_stat");
//        INCREMENTTABLE.add("ods_chinese_introduction.ods_search_history");
//        INCREMENTTABLE.add("ods_english_gold.ods_gd_gold_reward_detail");
//        INCREMENTTABLE.add("ods_point.point_etp_race_login_sign");
//        INCREMENTTABLE.add("ods_english_teacher.ods_tc_student_task");
//        INCREMENTTABLE.add("ods_kk_marketer.ods_thirdpartymsglog");
//        INCREMENTTABLE.add("ods_chinese_introduction.ods_browsing_history_log");
//        INCREMENTTABLE.add("ods_chinese_introduction.ods_user_behavior_log");
//        INCREMENTTABLE.add("ods_english_video.ods_vd_play_film");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_student_answer_exercise");
//        INCREMENTTABLE.add("ods_chinese_user_data.ods_rank_user_source_increase_log");
//        INCREMENTTABLE.add("ods_english_read.ods_rd_student_favorite_book");
//        INCREMENTTABLE.add("ods_chinese_power_value.ods_day_study_data_stat");
//        INCREMENTTABLE.add("ods_chinese_power_value.ods_study_milestone_evaluate_stat");
//        INCREMENTTABLE.add("ods_english_parent.ods_pt_parent_task");


        INCREMENTTABLE.add("english_game.gm_player_property_change");
        INCREMENTTABLE.add("english_read.rd_day_total_read");
        INCREMENTTABLE.add("chinese_power_value.word_study_stat");
        INCREMENTTABLE.add("english_ebook.ek_student_first_ebook");
        INCREMENTTABLE.add("english_read.rd_day_read");
        INCREMENTTABLE.add("english_video.hs_speak_student_answer");
        INCREMENTTABLE.add("chinese_article.article_sentence_info");
        INCREMENTTABLE.add("english_read.rd_through_book");
        INCREMENTTABLE.add("english_parent.pt_heart_talk");
        INCREMENTTABLE.add("english_read.rd_student_interrupt");
        INCREMENTTABLE.add("english_gold.gd_gold_bill");
        INCREMENTTABLE.add("english_game.gm_player_task");
        INCREMENTTABLE.add("english_video.vd_play_film_log");
        INCREMENTTABLE.add("english_oral.ol_day_oral");
        INCREMENTTABLE.add("chinese_article.read_article_info");
        INCREMENTTABLE.add("chinese_article.read_article_log");
        INCREMENTTABLE.add("english_oral.ol_day_oral_bak");
        INCREMENTTABLE.add("english_parent.pt_read_activity_sign_in");
        INCREMENTTABLE.add("english_student.st_student_view");
        INCREMENTTABLE.add("english_ebook.ek_student_ebook");
        INCREMENTTABLE.add("english_oral.ol_student_review");
        INCREMENTTABLE.add("english_student.st_student_report");
        INCREMENTTABLE.add("english_oral.ol_student_review_bak");
        INCREMENTTABLE.add("english_parent.pt_upload_sign_pic");
        INCREMENTTABLE.add("chinese_power_value.user_word_study_stat");
        INCREMENTTABLE.add("chinese_introduction.search_history");
        INCREMENTTABLE.add("english_gold.gd_gold_reward_detail");
        INCREMENTTABLE.add("point.point_etp_race_login_sign");
        INCREMENTTABLE.add("english_teacher.tc_student_task");
        INCREMENTTABLE.add("kk_marketer.thirdpartymsglog");
        INCREMENTTABLE.add("chinese_introduction.browsing_history_log");
        INCREMENTTABLE.add("chinese_introduction.user_behavior_log");
        INCREMENTTABLE.add("english_video.vd_play_film");
        INCREMENTTABLE.add("english_read.rd_student_answer_exercise");
        INCREMENTTABLE.add("chinese_user_data.rank_user_source_increase_log");
        INCREMENTTABLE.add("english_read.rd_student_favorite_book");
        INCREMENTTABLE.add("chinese_power_value.day_study_data_stat");
        INCREMENTTABLE.add("chinese_power_value.study_milestone_evaluate_stat");
        INCREMENTTABLE.add("english_parent.pt_parent_task");

        LinkedHashSet<TableInfo> allTabInfo = extractAllTabInfo();
//        hiveCreateTable(allTabInfo);
//        hiveCreateTable3(allTabInfo);
//        mysqlToHdfs(allTabInfo);
//        hiveLoadData(allTabInfo);
        //ods层
        hiveCreateTable2(allTabInfo);
        everyTableSqoopShell(allTabInfo);
        everyTableOdsShell(allTabInfo);
        azkabanOdsFlow(allTabInfo);
        //dwd
//        dwdCreateTable(allTabInfo);
//        azkabanDwdFlow(allTabInfo);


    }
}
