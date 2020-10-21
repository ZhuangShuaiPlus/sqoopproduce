package com.shuai;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashSet;
import java.util.LinkedList;

//shuai
public class SqoopProducter {

//    public static final String URL = "jdbc:mysql://172.28.30.28:3306";
//    public static final String USER = "zhengyajun";
//    public static final String PASSWORD = "zhengyajun8899";

//    public static final String URL = "jdbc:mysql://hadoop102:3306/gmall";
//    public static final String USER = "root";
//    public static final String PASSWORD = "123456";

    public static final String URL = "jdbc:mysql://10.16.40.154:3306/";
    public static final String USER = "zhuangshuai";
    public static final String PASSWORD = "0UtLSBLnYajUutJh";


//    public static final String URL = "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/";
//    public static final String USER = "qa_alldata_ro";
//    public static final String PASSWORD = "kUM8qL1cubf0Xf7RI9AT";

    public static final String PREFIX = "ods";
    public static final String ODSSHELLLOCATION = "/root/bin/azkabantest/hdfs_to_ods";
    public static final String SQOOPSHELLLOCATION = "/root/bin/azkabantest/oltp_to_hdfs";


    public static String sql = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
            "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
            "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
            "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
            "WHERE  a.TABLE_SCHEMA LIKE '" + PREFIX + "%' ;";
//            "WHERE  a.TABLE_SCHEMA = 'english_agent' ;";

    static LinkedHashSet<TableInfo> extractTabInfo() throws SQLException {
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


    static void everyTableOdsShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./hdfs_to_ods/" + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./hdfs_to_ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
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
            stringBuilder.append("hive -e \"load data inpath '/bigdata/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    static void everyTableSqoopShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {


        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./oltp_to_hdfs/" + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
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
                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
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

            for (FieldInfo fieldInfo : tableInfo.tableFields) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                if (tableInfo.tableFields.getLast() != fieldInfo) {
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
                    "create database if not exists " + tableInfo.getDatabaseName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "drop table if exists " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + ";" + "\n");
//            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append(
                    "create external table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "(  \n");

            for (FieldInfo fieldInfo : tableInfo.tableFields) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                if (tableInfo.tableFields.getLast() != fieldInfo) {
                    stringBuilder.append(" ,\n");
                } else {
                    stringBuilder.append("\n)\n");
                }
            }
            stringBuilder.append("COMMENT \"" + tableInfo.getTableComment() + "\"\n");
            stringBuilder.append(
                    "PARTITIONED BY (`dt` string) \n" +
                            "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/bigdata/warehouse/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";\n"
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

            for (FieldInfo fieldInfo : tableInfo.tableFields) {
                stringBuilder.append(
                        "`" + fieldInfo.getFieldName() + "`" + " " + fieldMap(fieldInfo.getDataType()) + " COMMENT " + "\"" + fieldInfo.getColumnComment() + "\" "
                );
                if (tableInfo.tableFields.getLast() != fieldInfo) {
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
                        "    echo \"Hive load data: error in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"Hive load data: successful in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }

    static void exceptionExitCreateTable(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \"Hive create table: error in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"Hive create table: successful in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }

    //sqoop error
    static void exceptionExitSqoop(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \"sqoop: error in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"sqoop: successful in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
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
                    "      command: sh " + SQOOPSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");


            stringBuilder.append("  - name: ods." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + ODSSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
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
        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo();
//        hiveCreateTable(allTabInfo);
        hiveCreateTable2(allTabInfo);
//        hiveCreateTable3(allTabInfo);
//        mysqlToHdfs(allTabInfo);
//        hiveLoadData(allTabInfo);
        everyTableSqoopShell(allTabInfo);
        everyTableOdsShell(allTabInfo);
        azkabanOdsFlow(allTabInfo);
    }
}
