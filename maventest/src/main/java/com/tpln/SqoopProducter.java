package com.tpln;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;

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

    //    生产Tidb
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
    public static final String LAYERPREFIX = "ods_";
    //    public static final String LAYERPREFIX = "";
    //    public static final String ODSSHELLLOCATION = "/root/bin/azkabantest/hdfs_to_ods";
    public static final String ODSSHELLLOCATION = "/root/bin/azkabanshell/hdfs_to_ods";
    public static final String DIMSHELLLOCATION = "/root/bin/azkabantest/hdfs_to_dim";
    public static final String DWDSHELLLOCATION = "/root/bin/azkabantest/ods_to_dwd";
    public static final String DWDINITSQOOPSHELLLOCATION = "/root/bin/azkabanshell/dwd_oltp_to_hdfs";
    public static final String DWDINITHIVESHELLLOCATION = "/root/bin/azkabanshell/dwd_hdfs_to_hive";
    public static final String DWMINITSQOOPSHELLLOCATION = "/root/bin/azkabanshell/dwm_oltp_to_hdfs";
    public static final String DWMINITHIVESHELLLOCATION = "/root/bin/azkabanshell/dwm_hdfs_to_hive";
    public static final String DWMSHELLLOCATION = "/root/bin/azkabantest/dwd_to_dwm";
    //    public static final String SQOOPSHELLLOCATION = "/root/bin/azkabantest/oltp_to_hdfs";
    public static final String SQOOPSHELLLOCATION = "/root/bin/azkabanshell/oltp_to_hdfs";
    public static final String DIMOLTPSHELLLOCATION = "/root/bin/azkabantest/dim_to_hdfs";

    //dws 的各层脚本

    //语文代理商-到tidb
    public static final String CHIAGENTSHELLLOCATION = "/root/bin/azkabantest/chiAgent_to_tidb";

    public static final HashSet<String> INCREMENTTABLE = new HashSet<String>();

    public static final HashSet<String> DIMTRUNCATEANDDATAVALUE = new HashSet<String>();


    public static String SQL = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
            "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
            "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
            "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
//            "WHERE  a.TABLE_SCHEMA LIKE '" + PREFIX + "%' ;";
//dwd层
//            "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' ;";
//            "WHERE  a.TABLE_SCHEMA = 'english_agent' ;";
            //kk的表
            "WHERE  a.TABLE_SCHEMA in (" +
            "ientrepreneurship_dev" + ");";

//    public static LinkedList<DataBaseInfo> DATALIST = new LinkedList<DataBaseInfo>();


//    static void init() throws SQLException {
//        DATALIST = new LinkedList<DataBaseInfo>();
//
//        String sqleng = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
//                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
//                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
//                "WHERE  a.TABLE_SCHEMA LIKE '" + "eng" + "%' ;";
//
//        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
//                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
//                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
//                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
//                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";
//
////        String sqlch = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
////                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
////                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
////                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
////                "WHERE  a.TABLE_SCHEMA LIKE '" + "chinese" + "%' ;";
//
//
//        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
//                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
//                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
//                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";
//
//        //英语测试
//        DATALIST.add(new DataBaseInfo(
//                "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/",
//                "qa_alldata_ro",
//                "kUM8qL1cubf0Xf7RI9AT",
//                sqleng
//        ));
//
//        //KK测试
//        DATALIST.add(new DataBaseInfo(
//                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
//                "agent_rw",
//                "ezuYvaJppO3kPmOgqqPY",
//                sqlkk
//        ));
////
////        //语文测试
////        datalist.add(new DataBaseInfo(
////                "jdbc:mysql://172.28.30.87:3306/",
////                "qa_new_chinese",
////                "Aa123456",
////                sqlch
////        ));
//
//
////        //dim测试-tidb
////        datalist.add(new DataBaseInfo(
////                "jdbc:mysql://172.28.30.28:3306/",
////                "zhengyajun",
////                "zhengyajun8899",
////                sqldim
////        ));
//
//
//        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();
//
////        for (DataBaseInfo dataBaseInfo : datalist) {
////            LinkedHashSet<TableInfo> tableInfos = extractTabInfo2(dataBaseInfo, dataBaseInfo.getSql());
////            allTabInfo.addAll(tableInfos);
////        }
////        return allTabInfo;
//    }


    static LinkedHashSet<TableInfo> extractTabInfo(String url, String user, String password, String sql) throws SQLException {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
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


    //与extractTabInfo的传参不同，一样
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
        //移除__drds__system__recyclebin__类
        fliterSpecialTable(allTabInfo);
        return allTabInfo;
    }


    static LinkedHashSet<TableInfo> extractTabInfoSpecifyDatabase(DataBaseInfo dataBaseInfo, String sql, String specifyDatabase) throws SQLException {
        //1.加载驱动程序
//        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword());
        Statement stmt = conn.createStatement();
        stmt.executeQuery(specifyDatabase);
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

        //移除__drds__system__recyclebin__类
        fliterSpecialTable(allTabInfo);
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

            stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    static void everyTableOdsIncrementShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
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
            stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "'" + " OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }

    //全量个增量厨师话用的
    static void everyTableOdsAllAndIncrementShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
//            if (!INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName())) {
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

                stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
                //azkaban报错用
                exceptionExitLoadData(stringBuilder, tableInfo);
                out.write(stringBuilder.toString());
                out.write("\n\n\n");
                out.close();
//            } else {
//                String dirLocation = "./hdfs_to_ods/" + LAYERPREFIX + tableInfo.getDatabaseName();
//                File f = new File(dirLocation);
//                if (!f.exists()) {
//                    f.mkdirs();
//                }
//                BufferedWriter out = new BufferedWriter(new FileWriter("./hdfs_to_ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
//                StringBuilder stringBuilder = new StringBuilder();
//                out.write("#!/bin/bash\n\n" +
//                        "if [ -n \"$1\" ] ;then\n" +
//                        "    do_date=$1\n" +
//                        "else\n" +
//                        "    do_date=`date -d '-1 day' +%F`\n" +
//                        "fi\n\n");
//                stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/ods/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "'" + " OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\"; \n\n");
//                //azkaban报错用
//                exceptionExitLoadData(stringBuilder, tableInfo);
//                out.write(stringBuilder.toString());
//                out.write("\n\n\n");
//                out.close();
//            }
        }
    }


    static void everyTableDwdShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./dwd_hdfs_to_hive/" + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./dwd_hdfs_to_hive/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");

            stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/dwd/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/' OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " \"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }


    static void everyTableDwmShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./dwm_hdfs_to_hive/" + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./dwm_hdfs_to_hive/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");

            stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/dwm/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/' OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " \"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }

    static void everyTableModuleShell(LinkedHashSet<TableInfo> allTabInfo, String layer) throws IOException {
        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./" + layer + "_hdfs_to_hive/" + tableInfo.getDatabaseName();
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./" + layer + "_hdfs_to_hive/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");

            stringBuilder.append("hive -n root -p root -e \"load data inpath '/bigdata/origin_data/db/" + layer + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/' OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " \"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }


    //dim第一次的load data
    static void everyTableDimShell(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
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
            stringBuilder.append("hive -e \"load data inpath '/bigdata/origin_data/db/dim/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "' OVERWRITE into table " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " \"; \n\n");
            //azkaban报错用
            exceptionExitLoadData(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    static void everyTableSqoopShell(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {


        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            odsSyncStrategy(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
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


    //sqoop加入--column列指定顺序
    static void everyTableSqoopShell_SpecifiedField(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            odsSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--hive-drop-import-delims \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 4 \\\n" +
                            "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
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


    static void everyTableSqoopShell_SpecifiedField_Increment(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password '" + dataBaseInfo.getPassword() + "' \\\n" +
                            odsSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/" + "" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--hive-drop-import-delims \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 4 \\\n" +
                            "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExitSqoop(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }

    }


    //增量与全量
    static void everyTableSqoopShell_SpecifiedField_allAndIncrement(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
//            if (!INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName())) {
                String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName());
                File f = new File(dirLocation);
                if (!f.exists()) {
                    f.mkdirs();
                }
                BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
                StringBuilder stringBuilder = new StringBuilder();
                out.write("#!/bin/bash\n\n" +
                        "if [ -n \"$1\" ] ;then\n" +
                        "    do_date=$1\n" +
                        "else\n" +
                        "    do_date=`date -d '-1 day' +%F`\n" +
                        "fi\n\n");
                stringBuilder.append(
                        "sqoop import \\\n" +
                                "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                                "-D mapreduce.task.timeout=7200000 \\\n" +
                                "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                                "--username " + dataBaseInfo.getUser() + " \\\n" +
                                "--password '" + dataBaseInfo.getPassword() + "' \\\n" +
                                odsSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                                "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/" + "$do_date" + " \\\n" +
                                "--delete-target-dir \\\n" +
                                "--hive-drop-import-delims \\\n" +
                                "--fields-terminated-by '\\001' \\\n" +
                                "--num-mappers 4 \\\n" +
                                "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
                                "--null-string '\\\\N' \\\n" +
                                "--null-non-string '\\\\N' \n\n"
                );
                exceptionExitSqoop(stringBuilder, tableInfo);
                out.write(stringBuilder.toString());
                out.write("\n\n\n");
                out.close();
//            } else {
//                String dirLocation = "./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName());
//                File f = new File(dirLocation);
//                if (!f.exists()) {
//                    f.mkdirs();
//                }
//                BufferedWriter out = new BufferedWriter(new FileWriter("./oltp_to_hdfs/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
//                StringBuilder stringBuilder = new StringBuilder();
//                out.write("#!/bin/bash\n\n" +
//                        "if [ -n \"$1\" ] ;then\n" +
//                        "    do_date=$1\n" +
//                        "else\n" +
//                        "    do_date=`date -d '-1 day' +%F`\n" +
//                        "fi\n\n");
//                stringBuilder.append(
//                        "sqoop import \\\n" +
//                                "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
//                                "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
//                                "--username " + dataBaseInfo.getUser() + " \\\n" +
//                                "--password " + dataBaseInfo.getPassword() + " \\\n" +
//                                odsSyncStrategy_SpecifiedField(tableInfo) +
//                                "--target-dir /bigdata/origin_data/db/ods/" + LAYERPREFIX + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/" + "" + " \\\n" +
//                                "--delete-target-dir \\\n" +
//                                "--hive-drop-import-delims \\\n" +
//                                "--fields-terminated-by '\\001' \\\n" +
//                                "--num-mappers 4 \\\n" +
//                                "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
//                                "--null-string '\\\\N' \\\n" +
//                                "--null-non-string '\\\\N' \n\n"
//                );
//                exceptionExitSqoop(stringBuilder, tableInfo);
//                out.write(stringBuilder.toString());
//                out.write("\n\n\n");
//                out.close();
//            }
        }
    }


    //dwd 迁移数据专用  为了还原分区所以去掉指定dt日期部分,迁移策略的字段顺序有问题
    static void everyTableSqoopShell_SpecifiedField_dwd(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./dwd_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./dwd_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            dwdSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/dwd/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/ \\\n" +
                            "--delete-target-dir \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 4 \\\n" +
                            "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExitSqoop(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }

    //dwm 迁移数据专用  为了还原分区所以去掉指定dt日期部分,迁移策略的字段顺序有问题
    static void everyTableSqoopShell_SpecifiedField_dwm(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./dwm_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./dwm_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            dwdSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/dwm/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/ \\\n" +
                            "--delete-target-dir \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 4 \\\n" +
                            "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExitSqoop(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }


    static void everyTableSqoopShell_SpecifiedField_module(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo, String layer) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./" + layer + "_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter("./" + layer + "_oltp_to_hdfs/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            out.write("#!/bin/bash\n\n" +
                    "if [ -n \"$1\" ] ;then\n" +
                    "    do_date=$1\n" +
                    "else\n" +
                    "    do_date=`date -d '-1 day' +%F`\n" +
                    "fi\n\n");
            stringBuilder.append(
                    "sqoop import \\\n" +
                            "-D org.apache.sqoop.splitter.allow_text_splitter=true \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            moduleSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/" + layer + "/" + standardization(tableInfo.getDatabaseName()) + "/" + tableInfo.getTableName() + "/ \\\n" +
                            "--delete-target-dir \\\n" +
                            "--hive-drop-import-delims \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 4 \\\n" +
                            "--split-by " + tableInfo.getTableFields().getFirst().getFieldName() + " \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExitSqoop(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }


    static void everyTableSqoopShell_dim(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

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
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            odsSyncStrategy(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/db/dim/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + " \\\n" +
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


    //指定列
    private static String odsSyncStrategy_SpecifiedField(TableInfo tableInfo) {

        if (!INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName())) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("--table " + tableInfo.getTableName() + " \\\n");
            stringBuilder.append("--columns ");
            LinkedList<FieldInfo> tableFields = tableInfo.getTableFields();
            int size = tableFields.size();
            for (FieldInfo fieldInfo : tableFields) {
                size--;
                stringBuilder.append(fieldInfo.getFieldName());
                if (size != 0) {
                    stringBuilder.append(",");
                }
            }
            stringBuilder.append(" \\\n");
            return stringBuilder.toString();
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
            stringBuilder.append(",date_format(createTime,'%Y-%m-%d')\n");
            stringBuilder.append("from " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n"
                    + "where date_format(createTime,'%Y-%m-%d') = \'$do_date\'\n"
                    + "and \\$CONDITIONS\" \\\n"
            );
            return stringBuilder.toString();
        }
    }


    //dwd指定列,换列的顺序,date_value放到最后
    private static String dwdSyncStrategy_SpecifiedField(TableInfo tableInfo) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("--table " + tableInfo.getTableName() + " \\\n");
        stringBuilder.append("--columns ");
        boolean is_add_date_value = false;
        int size = tableInfo.getTableFields().size();
        LinkedList<FieldInfo> tableFields = tableInfo.getTableFields();
        for (int i = 0; i < size; i++) {
            FieldInfo fieldInfo = tableFields.get(i);
            //最后一个若是date_value则也跳过
            if ("date_value".equals(fieldInfo.getFieldName()) && i + 1 != size) {
                is_add_date_value = true;
                continue;
            }
            stringBuilder.append(fieldInfo.getFieldName());
            if (i + 1 != size) {
                stringBuilder.append(",");
            }
        }
        if (is_add_date_value) {
            stringBuilder.append(",date_value");
        }
        stringBuilder.append(" \\\n");
        return stringBuilder.toString();
    }


    private static String moduleSyncStrategy_SpecifiedField(TableInfo tableInfo) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("--table " + tableInfo.getTableName() + " \\\n");
        stringBuilder.append("--columns ");
        boolean is_add_date_value = false;
        int size = tableInfo.getTableFields().size();
        LinkedList<FieldInfo> tableFields = tableInfo.getTableFields();
        for (int i = 0; i < size; i++) {
            FieldInfo fieldInfo = tableFields.get(i);
            //最后一个若是date_value则也跳过
            if ("date_value".equals(fieldInfo.getFieldName()) && i + 1 != size) {
                is_add_date_value = true;
                continue;
            }
            stringBuilder.append(fieldInfo.getFieldName());
            if (i + 1 != size) {
                stringBuilder.append(",");
            }
        }
        if (is_add_date_value) {
            stringBuilder.append(",date_value");
        }
        stringBuilder.append(" \\\n");
        return stringBuilder.toString();
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

    //hive -e 的参数过长，弃用
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


    //每50个表进行一次hive-e,解决建表参数过长的问题
    static void hiveCreateTable2(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table.sh"));

        out.write("hive -n root -p root -e '");

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
                out.write("';\n hive -n root -p root -e '\n");
            }
            i++;

        }
        out.write("';\n");

        out.close();
    }



    //特殊用：移除多的分区
    static void removePartitionOdsTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("remove_partition_ods_table.sh"));
        out.write("hive -n root -p root -e '");
        int i = 0;
        for (TableInfo tableInfo : allTabInfo) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("ALTER TABLE " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " DROP IF EXISTS PARTITION(dt=\"2020-11-28\");" + "\n");
            out.write(stringBuilder.toString());
            if (i % 50 == 0) {
                out.write("';\n hive -n root -p root -e '\n");
            }
            i++;
        }
        out.write("';\n");
        out.close();
    }

    static void newChiCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("newChi_create_table.sh"));
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


    //KK 建表 只是文件名改变，后期可以直接使用hivecreatetable2包装
    static void hiveCreateTable_KK(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table_KK.sh"));

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

    //老chi 建表 只是文件名改变，后期可以直接使用hivecreatetable2包装
    static void hiveCreateTable_chi(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("hive_create_table_chi.sh"));

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

    //去掉单引号和双引号
    static String fliterQuotation(String s) throws IOException {
//        if (s.contains("\'") || s.contains("\"")) {
        s = s.replaceAll("\"", "");
        s = s.replaceAll("\'", "");
//            return s.replaceAll("\"", "");
//        }
        return s;
    }


    //标准化-把横线转换为下划线
    static String standardization(String s) {
        return fliterLine(s);
    }

    //把横线转换为下划线
    static String fliterLine(String s) {
        String ss = s.replace("-", "_");
        return ss;
    }


    //dim的建表
    static void dimCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter("dim_create_table.sh"));

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
                    "row format delimited fields terminated by \"\\001\" \n"
                            + "location \"/bigdata/warehouse/dim/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";\n"
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


    //dwd的建表语句 比平常的建表的语句多了分区字段的特数处理（date_value）
    static void dwdCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("dwd_create_table.sh"));
        for (TableInfo tableInfo : allTabInfo) {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("hive -n root -p root -e '");
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


    //loaddata时候的策略
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
        if ("date".equals(split[0]) || "datetime".equals(split[0]) || "timestamp".equals(split[0])) {
            hiveDataType = "string";
        } else if ("tinyint".equals(split[0]) || "smallint".equals(split[0]) || "int".equals(split[0]) || "bigint".equals(split[0]) || "float".equals(split[0]) || "double".equals(split[0])) {
            hiveDataType = split[0];
        } else if ("mediumint".equals(split[0])) {
            hiveDataType = "int";
        } else if ("varchar".equals(split[0]) || "text".equals(split[0]) || "mediumtext".equals(split[0]) || "longtext".equals(split[0])) {
            hiveDataType = "string";
        } else if ("bit".equals(split[0])) {
            hiveDataType = "boolean";
        } else if ("json".equals(split[0])) {
            hiveDataType = "string";
        } else {
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
                        "    echo \"Hive create table: error in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \"Hive create table: successful in " + LAYERPREFIX + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "fi\n\n"
        );
    }

    static void moduleExceptionExitCreateTable(StringBuilder stringBuilder, TableInfo tableInfo) {
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

    static void exceptionExit(StringBuilder stringBuilder, TableInfo tableInfo) {
        stringBuilder.append(
                "result=$? \n" +
                        "echo \"result = $result\" \n" +
                        "if [ $result -ne 0 ] ; then \n" +
                        "    echo \" error in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
                        "    exit $result\n" +
                        "else\n" +
                        "    echo \" successful in " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\" \n" +
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


    static void azkabanNewChiFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./newChi-azkaban-schedule/";
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


    static void azkabanChiAgentFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./chiAgent-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "chiAgent.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "chiAgent.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: hdfs." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    config:\n" +
                    "      command: sh " + CHIAGENTSHELLLOCATION + "/hdfs/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");


            stringBuilder.append("  - name: tidb." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "hdfs." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + CHIAGENTSHELLLOCATION + "/tidb/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
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


    static void azkabanDimFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./dim-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "dim_schedule.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "dim_schedule.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    config:\n" +
                    "      command: sh " + DIMOLTPSHELLLOCATION + "/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");


            stringBuilder.append("  - name: dim." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "sqoop." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + DIMSHELLLOCATION + "/" + LAYERPREFIX + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
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
                    "      command: sh " + DWDSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }


    static void azkabanDwdInitFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./dwd-init-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "dwd-init_schedule.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "dwd-init_schedule.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: sqoopdwd." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    config:\n" +
                    "      command: sh " + DWDINITSQOOPSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");

            stringBuilder.append("  - name: hivedwd." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "sqoopdwd." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + DWDINITHIVESHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }

    static void azkabanModuleInitFlow(LinkedHashSet<TableInfo> allTabInfo, String layer, String shellSqoopLocation, String shellHiveLocation) throws IOException {
        //创建所需目录
        String dirLocation = "./" + layer + "-init-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + layer + "-init_schedule.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + layer + "-init_schedule.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: sqoop" + layer + "." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    config:\n" +
                    "      command: sh " + shellSqoopLocation + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");

            stringBuilder.append("  - name: hive" + layer + "." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "sqoop" + layer + "." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    config:\n" +
                    "      command: sh " + shellHiveLocation + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

        out.write(stringBuilder.toString());
        out.write("\n\n\n");
        out.close();
    }


    static void azkabanDwmFlow(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        //创建所需目录
        String dirLocation = "./dwm-azkaban-schedule/";
        File f = new File(dirLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
        //生成azkaban的执行工程
        BufferedWriter out2 = new BufferedWriter(new FileWriter(dirLocation + "dwm_schedule.project"));
        out2.write("azkaban-flow-version: 2.0");

        out2.close();
        BufferedWriter out = new BufferedWriter(new FileWriter(dirLocation + "dwm_schedule.flow"));
        out.write("nodes:\n");

        StringBuilder stringBuilder = new StringBuilder();
        for (TableInfo tableInfo : allTabInfo) {
            stringBuilder.append("  - name: dwm." + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\n" +
                    "    type: command\n" +
                    "    dependsOn:\n" +
                    "      - " + "dwd." + "\n" +
                    "    config:\n" +
                    "      command: sh " + DWMSHELLLOCATION + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh\n\n");
        }

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

    //把库名标准化，比如把-换成_
    static LinkedHashSet<TableInfo> standardization(LinkedHashSet<TableInfo> allTabInfo) {

        for (TableInfo tableInfo : allTabInfo) {
            tableInfo.setDatabaseName(fliterLine(tableInfo.getDatabaseName()));
        }
        return allTabInfo;
    }

    static void chiAgentEveryTableSqoopShell(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {


        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./chiAgent_to_tidb/hdfs/" + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }

            BufferedWriter out = new BufferedWriter(new FileWriter("./chiAgent_to_tidb/hdfs/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append(
                    "sqoop import \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            odsSyncStrategy_SpecifiedField(tableInfo) +
//                            "--table " + tableInfo.getTableName() + " \\\n" +
                            "--target-dir /bigdata/origin_data/tmp/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + " \\\n" +
                            "--delete-target-dir \\\n" +
                            "--hive-drop-import-delims \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 1 \\\n" +
                            "--null-string '\\\\N' \\\n" +
                            "--null-non-string '\\\\N' \n\n"
            );
            exceptionExit(stringBuilder, tableInfo);
//            stringBuilder.append("\n");
//            stringBuilder.append("hive -e \"load data inpath '/user/origin_data/db/ods/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/" + "$do_date'" + " OVERWRITE into table " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + " partition(dt='$do_date')\"; \n\n");
            //azkaban报错用
//            exceptionExit(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }


    static void chiAgentEveryTableTidbShell(LinkedHashSet<TableInfo> allTabInfo, DataBaseInfo dataBaseInfo) throws IOException {

        for (TableInfo tableInfo : allTabInfo) {
            String dirLocation = "./chiAgent_to_tidb/tidb/" + standardization(tableInfo.getDatabaseName());
            File f = new File(dirLocation);
            if (!f.exists()) {
                f.mkdirs();
            }

            BufferedWriter out = new BufferedWriter(new FileWriter("./chiAgent_to_tidb/tidb/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + ".sh"));
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("mysql -h 172.28.30.28 -P 3306  -u zhengyajun -pzhengyajun8899 -e \"truncate " + tableInfo.getDatabaseName() + "." + tableInfo.getTableName() + "\"");
            stringBuilder.append("\n\n");
            exceptionExit(stringBuilder, tableInfo);
            stringBuilder.append("\n");
            stringBuilder.append(
                    "sqoop export \\\n" +
                            "--connect " + dataBaseInfo.getUrl() + tableInfo.getDatabaseName() + " \\\n" +
                            "--username " + dataBaseInfo.getUser() + " \\\n" +
                            "--password " + dataBaseInfo.getPassword() + " \\\n" +
                            odsSyncStrategy(tableInfo) +
                            "--export-dir /bigdata/origin_data/tmp/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/ \\\n" +
                            "--fields-terminated-by '\\001' \\\n" +
                            "--num-mappers 1 \\\n" +
                            "--input-null-string '\\\\N' \\\n" +
                            "--input-null-non-string '\\\\N' \n\n"
            );
            exceptionExit(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n\n");
            out.close();
        }
    }

    static void chineseRds_to_Tidb() throws SQLException, IOException {
        String sqlChiAgent = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME \n" +
                "WHERE   a.TABLE_SCHEMA = 'cn_account' \n" +
                "OR a.TABLE_SCHEMA = 'cn_marketer' \n" +
                "OR a.TABLE_SCHEMA = 'cn_member' " +
                ";";


        DataBaseInfo chiAgent = new DataBaseInfo(
                "jdbc:mysql://rm-2zekupo10c175t4q1.mysql.rds.aliyuncs.com:3306/",
                "qa_root",
                "P052ao79mBwDxN8fzesH",
                sqlChiAgent
        );


        String sqleng = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE   a.TABLE_SCHEMA = 'english_video' \n" +
                "OR a.TABLE_SCHEMA = 'english_agent' \n" +
                "OR a.TABLE_SCHEMA = 'english_read' \n" +
                "OR a.TABLE_SCHEMA = 'english_parent' \n" +
                "OR a.TABLE_SCHEMA = 'english_student' " +
                ";";


        DataBaseInfo eng = new DataBaseInfo(
                "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/",
                "qa_alldata_ro",
                "kUM8qL1cubf0Xf7RI9AT",
                sqleng
        );


        DataBaseInfo tidbTest = new DataBaseInfo(
                "jdbc:mysql://172.28.30.28:3306/",
                "zhengyajun",
                "zhengyajun8899",
                null
        );

        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(chiAgent, sqlChiAgent);
        chiAgentEveryTableSqoopShell(allTabInfo, chiAgent);

        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(eng, sqleng);
        chiAgentEveryTableSqoopShell(allTabInfo2, eng);

        allTabInfo.addAll(allTabInfo2);
        chiAgentEveryTableTidbShell(allTabInfo, tidbTest);
        azkabanChiAgentFlow(allTabInfo);

    }


    //ambanri
    static void ambanri() throws SQLException, IOException, InterruptedException {


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
//        INCREMENTTABLE.add("kk_marketer.thirdpartymsglog");//没有createtime的字段
        INCREMENTTABLE.add("chinese_introduction.browsing_history_log");
        INCREMENTTABLE.add("chinese_introduction.user_behavior_log");
        INCREMENTTABLE.add("english_video.vd_play_film");
        INCREMENTTABLE.add("english_read.rd_student_answer_exercise");
        INCREMENTTABLE.add("chinese_user_data.rank_user_source_increase_log");
        INCREMENTTABLE.add("english_read.rd_student_favorite_book");
        INCREMENTTABLE.add("chinese_power_value.english_student.st_student_view");
        INCREMENTTABLE.add("chinese_power_value.study_milestone_evaluate_stat");
        INCREMENTTABLE.add("english_parent.pt_parent_task");

        //-----------------------------------------------------
        String sqleng = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "eng" + "%' ;";

        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";

        String sqlchi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment,\n" +
                "row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-user-login' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-crawl' ;";


        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";

        String sqldwd = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' " +
                "and a.TABLE_NAME like 'dwd%' ;";

        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        //------------------------------------------------------------------


//        LinkedHashSet<TableInfo> allTabInfo = extractAllTabInfo();

        DataBaseInfo eng = new DataBaseInfo(
                "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/",
                "qa_alldata_ro",
                "kUM8qL1cubf0Xf7RI9AT",
                sqleng
        );

        DataBaseInfo dwd = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwd
        );

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );


        DataBaseInfo kk = new DataBaseInfo(
                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "LALQJGpfiw1jUyLhTqu6",
                sqlkk
        );

        DataBaseInfo chi = new DataBaseInfo(
                "jdbc:mysql://tope-qa-all-bj.rwlb.rds.aliyuncs.com:3306/",
                "chin_all_mgr",
                "fBwR3RMFeZ9RyNZC#v",
                sqlchi
        );

        DataBaseInfo dim = new DataBaseInfo(
                "jdbc:mysql://172.28.30.28:3306/",
                "zhengyajun",
                "zhengyajun8899",
                sqldim
        );

        //ods层
//        LinkedHashSet<TableInfo> allTabInfo3 = extractTabInfo2(eng, sqleng);
//        everyTableSqoopShell_SpecifiedField(allTabInfo3, eng);
//        hiveCreateTable2(allTabInfo3);
//
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(kk, sqlkk);
//        everyTableSqoopShell_SpecifiedField(allTabInfo, kk);
//        hiveCreateTable_KK(allTabInfo);
//
//        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(chi, sqlchi);
//        everyTableSqoopShell_SpecifiedField(allTabInfo2, chi);
////        standardization(allTabInfo2);
////        hiveCreateTable_chi(allTabInfo2);
//        allTabInfo.addAll(allTabInfo2);
//        allTabInfo.addAll(allTabInfo3);
////
//        everyTableOdsShell(allTabInfo);
//        azkabanOdsFlow(allTabInfo);


        //dwd调度
//        LinkedHashSet<TableInfo> allTabInfo4 = extractTabInfo2(dwd, sqldwd);
//        azkabanDwdFlow(allTabInfo4);

        //dwm调度
        LinkedHashSet<TableInfo> allTabInfo5 = extractTabInfo2(dwm, sqldwm);
        azkabanDwmFlow(allTabInfo5);

//        everyTableOdsShell(allTabInfo2);
//        azkabanOdsFlow(allTabInfo2);


//        everyTableOdsShell(allTabInfo);
//        azkabanOdsFlow(allTabInfo);

//        LinkedHashMap<String,Integer> map =  new  LinkedHashMap<String,Integer>();
//        for (TableInfo tableInfo : allTabInfo2) {
//
//            if(map.containsKey(tableInfo.getDatabaseName()+"."+tableInfo.getTableName())){
//                map.put(tableInfo.getDatabaseName()+"."+tableInfo.getTableName(),map.get(tableInfo.getDatabaseName()+"."+tableInfo.getTableName())+1);
//            }else {
//
//                map.put(tableInfo.getDatabaseName()+"."+tableInfo.getTableName(),0);
//            }
//
//
//        }

//        Thread.sleep(10);


        //dwd
//        dwdCreateTable(allTabInfo);


        //dim
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(dim, sqldim);
//        dimCreateTable(allTabInfo);
//        everyTableSqoopShell_dim(allTabInfo, dim);
//        everyTableDimShell(allTabInfo);
//        azkabanDimFlow(allTabInfo);

        //kk
//        hiveCreateTable2(allTabInfo);
    }


    //cdh的
    static void cdh() throws SQLException, IOException, InterruptedException {
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
//        INCREMENTTABLE.add("kk_marketer.thirdpartymsglog");//没有createtime的字段
        INCREMENTTABLE.add("chinese_introduction.browsing_history_log");
        INCREMENTTABLE.add("chinese_introduction.user_behavior_log");
        INCREMENTTABLE.add("english_video.vd_play_film");
        INCREMENTTABLE.add("english_read.rd_student_answer_exercise");
        INCREMENTTABLE.add("chinese_user_data.rank_user_source_increase_log");
        INCREMENTTABLE.add("english_read.rd_student_favorite_book");
        INCREMENTTABLE.add("chinese_power_value.day_study_data_stat");
        INCREMENTTABLE.add("chinese_power_value.study_milestone_evaluate_stat");
        INCREMENTTABLE.add("english_parent.pt_parent_task");

        //-----------------------------------------------------
        String sqleng = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "eng" + "%' ;";

        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";

        String sqlchi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment,\n" +
                "row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-user-login' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-crawl' ;";


        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";

        String sqldwd = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' " +
                "and a.TABLE_NAME like 'dwd%' ;";

        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        //------------------------------------------------------------------


//        LinkedHashSet<TableInfo> allTabInfo = extractAllTabInfo();

        DataBaseInfo eng = new DataBaseInfo(
                "jdbc:mysql://drdsbggaprundd1i.drds.aliyuncs.com:3306/",
                "qa_alldata_ro",
                "kUM8qL1cubf0Xf7RI9AT",
                sqleng
        );

        DataBaseInfo dwd = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwd
        );

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );


        DataBaseInfo kk = new DataBaseInfo(
                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "LALQJGpfiw1jUyLhTqu6",
                sqlkk
        );

        DataBaseInfo chi = new DataBaseInfo(
                "jdbc:mysql://tope-qa-all-bj.rwlb.rds.aliyuncs.com:3306/",
                "chin_all_mgr",
                "fBwR3RMFeZ9RyNZC#v",
                sqlchi
        );

        DataBaseInfo dim = new DataBaseInfo(
                "jdbc:mysql://172.28.30.28:3306/",
                "zhengyajun",
                "zhengyajun8899",
                sqldim
        );

        //ods层
//        LinkedHashSet<TableInfo> allTabInfo3 = extractTabInfo2(eng, sqleng);
//        everyTableSqoopShell_SpecifiedField(allTabInfo3, eng);
//        hiveCreateTable2(allTabInfo3);
//
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(kk, sqlkk);
//        everyTableSqoopShell_SpecifiedField(allTabInfo, kk);
//        hiveCreateTable_KK(allTabInfo);
//
//        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(chi, sqlchi);
//        everyTableSqoopShell_SpecifiedField(allTabInfo2, chi);
////        standardization(allTabInfo2);
////        hiveCreateTable_chi(allTabInfo2);
//        allTabInfo.addAll(allTabInfo2);
//        allTabInfo.addAll(allTabInfo3);
////
//        everyTableOdsShell(allTabInfo);
//        azkabanOdsFlow(allTabInfo);


        //dwd调度
        LinkedHashSet<TableInfo> allTabInfo4 = extractTabInfo2(dwd, sqldwd);
//        azkabanDwdFlow(allTabInfo4);

        //dwm调度
//        LinkedHashSet<TableInfo> allTabInfo5 = extractTabInfo2(dwm, sqldwm);
//        azkabanDwmFlow(allTabInfo5);

//        everyTableOdsShell(allTabInfo2);
//        azkabanOdsFlow(allTabInfo2);


//        everyTableOdsShell(allTabInfo);
//        azkabanOdsFlow(allTabInfo);

//        LinkedHashMap<String,Integer> map =  new  LinkedHashMap<String,Integer>();
//        for (TableInfo tableInfo : allTabInfo2) {
//
//            if(map.containsKey(tableInfo.getDatabaseName()+"."+tableInfo.getTableName())){
//                map.put(tableInfo.getDatabaseName()+"."+tableInfo.getTableName(),map.get(tableInfo.getDatabaseName()+"."+tableInfo.getTableName())+1);
//            }else {
//
//                map.put(tableInfo.getDatabaseName()+"."+tableInfo.getTableName(),0);
//            }
//
//
//        }

//        Thread.sleep(10);


        //dwd
        dwdCreateTable(allTabInfo4);
        everyTableSqoopShell_SpecifiedField(allTabInfo4, dwd);

        everyTableOdsShell(allTabInfo4);


        //dim
//        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(dim, sqldim);
//        dimCreateTable(allTabInfo);
//        everyTableSqoopShell_dim(allTabInfo, dim);
//        everyTableDimShell(allTabInfo);
//        azkabanDimFlow(allTabInfo);

        //kk
//        hiveCreateTable2(allTabInfo);
    }


    static void init() {
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
//        INCREMENTTABLE.add("kk_marketer.thirdpartymsglog");//没有createtime的字段
        INCREMENTTABLE.add("chinese_introduction.browsing_history_log");
        INCREMENTTABLE.add("chinese_introduction.user_behavior_log");
        INCREMENTTABLE.add("english_video.vd_play_film");
        INCREMENTTABLE.add("english_read.rd_student_answer_exercise");
        INCREMENTTABLE.add("chinese_user_data.rank_user_source_increase_log");
        INCREMENTTABLE.add("english_read.rd_student_favorite_book");
        INCREMENTTABLE.add("chinese_power_value.day_study_data_stat");
        INCREMENTTABLE.add("chinese_power_value.study_milestone_evaluate_stat");
        INCREMENTTABLE.add("english_parent.pt_parent_task");
    }

    static void prodOds() throws SQLException, IOException {

        //-----------------------english------------------------------
        String sql_english_read = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_read" + "' ;";

        String sql_english_oral = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_oral" + "' ;";

        String sql_english_rank_ebook_gold = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_rank" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_ebook'\n" +
                "or a.TABLE_SCHEMA  = 'english_gold';";

//        String sql_english_rank_ebook_gold = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
//                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
//                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
//                "WHERE  a.TABLE_SCHEMA  = '" + "english_rank" + "' \n" +
//                "or a.TABLE_SCHEMA  = 'english_ebook'\n" +
//                "or a.TABLE_SCHEMA  = 'english_gold';";

        String sql_english_game_etp = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_game" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_etp';";

        String sql_english_basedata_agent_book_parent_skill = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_basedata" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_agent'\n" +
                "or a.TABLE_SCHEMA  = 'english_book'\n" +
                "or a.TABLE_SCHEMA  = 'english_parent'\n" +
                "or a.TABLE_SCHEMA  = 'english_skill';";

        String sql_english_student_teache_video = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_student" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_teacher'\n" +
                "or a.TABLE_SCHEMA  = 'english_video';";

        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";

        String sql_prod_kk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship' ;";

        String sqlchi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment,\n" +
                "row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-user-login' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-crawl' ;";

        //线上为rds 无需进行排序
        String sql_prod_old_chi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                ",row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' ;";

        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";

        String sqldwd = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' " +
                "and a.TABLE_NAME like 'dwd%' ;";

        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        //------------------------------------------------------------------


//        LinkedHashSet<TableInfo> allTabInfo = extractAllTabInfo();

        DataBaseInfo english_read = new DataBaseInfo(
                "jdbc:mysql://drdsbggado8i32w8.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_read
        );
        DataBaseInfo english_oral = new DataBaseInfo(
                "jdbc:mysql://drdsbgga1s358k06.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_oral
        );
        DataBaseInfo english_rank_ebook_gold = new DataBaseInfo(
                "jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_rank_ebook_gold
        );
        DataBaseInfo english_game_etp = new DataBaseInfo(
                "jdbc:mysql://drdsbgga17222szn.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_game_etp
        );

        DataBaseInfo english_basedata_agent_book_parent_skill = new DataBaseInfo(
                "jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_basedata_agent_book_parent_skill
        );

        DataBaseInfo english_student_teache_video = new DataBaseInfo(
                "jdbc:mysql://drdsbgga581301b3.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_student_teache_video
        );


        DataBaseInfo dwd = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwd
        );

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );


        DataBaseInfo kk = new DataBaseInfo(
                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "LALQJGpfiw1jUyLhTqu6",
                sqlkk
        );

        DataBaseInfo kkProd = new DataBaseInfo(
                "jdbc:mysql://rm-2zeo7cx08119h573m.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_prod_kk
        );

        DataBaseInfo chi = new DataBaseInfo(
                "jdbc:mysql://tope-qa-all-bj.rwlb.rds.aliyuncs.com:3306/",
                "chin_all_mgr",
                "fBwR3RMFeZ9RyNZC#v",
                sqlchi
        );

        DataBaseInfo chiOldProd = new DataBaseInfo(
                "jdbc:mysql://rm-2ze50a5xi3885jkkc.mysql.rds.aliyuncs.com:3306/",
                "tidb_root",
                "PRb0K_oIV6cIPj2ExsRk",
                sql_prod_old_chi
        );

        DataBaseInfo dim = new DataBaseInfo(
                "jdbc:mysql://172.28.30.28:3306/",
                "zhengyajun",
                "zhengyajun8899",
                sqldim
        );

//        LinkedHashSet<TableInfo> allTabInfo6 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video,"use english_teacher;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo6, english_student_teache_video);
//        everyTableOdsShell(allTabInfo6);

//        LinkedHashSet<TableInfo> allTabInfo1 = extractTabInfo2(english_read, sql_english_read);
//        everyTableSqoopShell_SpecifiedField(allTabInfo1, english_read);
//        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(english_oral, sql_english_oral);
//        everyTableSqoopShell_SpecifiedField(allTabInfo2, english_oral);
//
//        LinkedHashSet<TableInfo> allTabInfo3_1 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_game;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo3_1, english_game_etp);
//        LinkedHashSet<TableInfo> allTabInfo3_2 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_etp;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo3_2, english_game_etp);
//
//        LinkedHashSet<TableInfo> allTabInfo4_1 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_basedata;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo4_1, english_basedata_agent_book_parent_skill);
//        LinkedHashSet<TableInfo> allTabInfo4_2 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_agent;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo4_2, english_basedata_agent_book_parent_skill);
//        LinkedHashSet<TableInfo> allTabInfo4_3 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_book;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo4_3, english_basedata_agent_book_parent_skill);
//        LinkedHashSet<TableInfo> allTabInfo4_4 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_parent;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo4_4, english_basedata_agent_book_parent_skill);
//        LinkedHashSet<TableInfo> allTabInfo4_5 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_skill;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo4_5, english_basedata_agent_book_parent_skill);
//
//        LinkedHashSet<TableInfo> allTabInfo5_1 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_rank;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo5_1, english_rank_ebook_gold);
//        LinkedHashSet<TableInfo> allTabInfo5_2 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_ebook;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo5_2, english_rank_ebook_gold);
//        LinkedHashSet<TableInfo> allTabInfo5_3 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_gold;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo5_3, english_rank_ebook_gold);
//
//        LinkedHashSet<TableInfo> allTabInfo6_1 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_student;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo6_1, english_student_teache_video);
//        LinkedHashSet<TableInfo> allTabInfo6_2 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_teacher;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo6_2, english_student_teache_video);
//        LinkedHashSet<TableInfo> allTabInfo6_3 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_video;");
//        everyTableSqoopShell_SpecifiedField(allTabInfo6_3, english_student_teache_video);
//
//        LinkedHashSet<TableInfo> allTabInfo7 = extractTabInfo2(chiOldProd, sql_prod_old_chi);
//        everyTableSqoopShell_SpecifiedField(allTabInfo7, chiOldProd);
//
//        LinkedHashSet<TableInfo> allTabInfo8 = extractTabInfo2(kkProd, sql_prod_kk);
//        everyTableSqoopShell_SpecifiedField(allTabInfo8, kkProd);


        LinkedHashSet<TableInfo> allTabInfo1 = extractTabInfo2(english_read, sql_english_read);
        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(english_oral, sql_english_oral);
        LinkedHashSet<TableInfo> allTabInfo3_1 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_game;");
        LinkedHashSet<TableInfo> allTabInfo3_2 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_etp;");
        LinkedHashSet<TableInfo> allTabInfo4_1 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_basedata;");
        LinkedHashSet<TableInfo> allTabInfo4_2 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_agent;");
        LinkedHashSet<TableInfo> allTabInfo4_3 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_book;");
        LinkedHashSet<TableInfo> allTabInfo4_4 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_parent;");
        LinkedHashSet<TableInfo> allTabInfo4_5 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_skill;");
        LinkedHashSet<TableInfo> allTabInfo5_1 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_rank;");
        LinkedHashSet<TableInfo> allTabInfo5_2 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_ebook;");
        LinkedHashSet<TableInfo> allTabInfo5_3 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_gold;");
        LinkedHashSet<TableInfo> allTabInfo6_1 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_student;");
        LinkedHashSet<TableInfo> allTabInfo6_2 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_teacher;");
        LinkedHashSet<TableInfo> allTabInfo6_3 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_video;");
        LinkedHashSet<TableInfo> allTabInfo7 = extractTabInfo2(chiOldProd, sql_prod_old_chi);
        LinkedHashSet<TableInfo> allTabInfo8 = extractTabInfo2(kkProd, sql_prod_kk);


        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo1, english_read);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo2, english_oral);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo3_1, english_game_etp);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo3_2, english_game_etp);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo4_1, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo4_2, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo4_3, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo4_4, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo4_5, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo5_1, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo5_2, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo5_3, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo6_1, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo6_2, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo6_3, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo7, chiOldProd);
        everyTableSqoopShell_SpecifiedField_Increment(allTabInfo8, kkProd);

        everyTableOdsShell(allTabInfo1);
        everyTableOdsShell(allTabInfo2);
        everyTableOdsShell(allTabInfo3_1);
        everyTableOdsShell(allTabInfo3_2);
        everyTableOdsShell(allTabInfo4_1);
        everyTableOdsShell(allTabInfo4_2);
        everyTableOdsShell(allTabInfo4_3);
        everyTableOdsShell(allTabInfo4_4);
        everyTableOdsShell(allTabInfo4_5);
        everyTableOdsShell(allTabInfo5_1);
        everyTableOdsShell(allTabInfo5_2);
        everyTableOdsShell(allTabInfo5_3);
        everyTableOdsShell(allTabInfo6_1);
        everyTableOdsShell(allTabInfo6_2);
        everyTableOdsShell(allTabInfo6_3);
        everyTableOdsShell(allTabInfo7);
        everyTableOdsShell(allTabInfo8);


        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();
        allTabInfo.addAll(allTabInfo1);
        allTabInfo.addAll(allTabInfo2);
        allTabInfo.addAll(allTabInfo3_1);
        allTabInfo.addAll(allTabInfo3_2);
        allTabInfo.addAll(allTabInfo4_1);
        allTabInfo.addAll(allTabInfo4_2);
        allTabInfo.addAll(allTabInfo4_3);
        allTabInfo.addAll(allTabInfo4_4);
        allTabInfo.addAll(allTabInfo4_5);
        allTabInfo.addAll(allTabInfo5_1);
        allTabInfo.addAll(allTabInfo5_2);
        allTabInfo.addAll(allTabInfo5_3);
        allTabInfo.addAll(allTabInfo6_1);
        allTabInfo.addAll(allTabInfo6_2);
        allTabInfo.addAll(allTabInfo6_3);
        allTabInfo.addAll(allTabInfo7);
        allTabInfo.addAll(allTabInfo8);

        hiveCreateTable2(allTabInfo);
        azkabanOdsFlow(allTabInfo);
    }

    //增量数据所有的历史数据放到第一天分区中
    static void prodOdsAllAndIncrement() throws SQLException, IOException {

        //-----------------------english------------------------------
        String sql_english_read = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_read" + "' ;";

        String sql_english_oral = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_oral" + "' ;";

        String sql_english_rank_ebook_gold = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_rank" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_ebook'\n" +
                "or a.TABLE_SCHEMA  = 'english_gold';";

//        String sql_english_rank_ebook_gold = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
//                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
//                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
//                "WHERE  a.TABLE_SCHEMA  = '" + "english_rank" + "' \n" +
//                "or a.TABLE_SCHEMA  = 'english_ebook'\n" +
//                "or a.TABLE_SCHEMA  = 'english_gold';";

        String sql_english_game_etp = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_game" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_etp';";

        String sql_english_basedata_agent_book_parent_skill = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_basedata" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_agent'\n" +
                "or a.TABLE_SCHEMA  = 'english_book'\n" +
                "or a.TABLE_SCHEMA  = 'english_parent'\n" +
                "or a.TABLE_SCHEMA  = 'english_skill';";

        String sql_english_student_teache_video = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA  = '" + "english_student" + "' \n" +
                "or a.TABLE_SCHEMA  = 'english_teacher'\n" +
                "or a.TABLE_SCHEMA  = 'english_video';";

        String sqlkk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship_dev' ;";

        String sql_prod_kk = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "kk" + "%' \n" +
                "or " + "a.TABLE_SCHEMA = 'ientrepreneurship' ;";

        String sqlchi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment,\n" +
                "row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-user-login' \n" +
                "AND a.`TABLE_SCHEMA`!= 'chinese-crawl' ;";

        //线上为rds 无需进行排序
        String sql_prod_old_chi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
//                ",row_number() over(PARTITION BY a.`TABLE_SCHEMA`,a.`TABLE_NAME` ORDER BY a.`ORDINAL_POSITION` ) AS num\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "chi" + "%' ;";

        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";

        String sqldwd = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' " +
                "and a.TABLE_NAME like 'dwd%' ;";

        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        //-------新加入的库-----------------------------------------------------------

        String sql_prod_new_chi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "cn_" + "%' ;";

        String sql_prod_english_etp_race = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA = '" + "english_etp_race" + "' ;";

        String sql_prod_shop = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA = '" + "pintuan_shop" + "' ;";

        String sql_prod_english_addressbook = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA = '" + "english_addressbook" + "' ;";

        String sql_prod_user_product = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_NAME = '" + "user_product" + "' ;";



        DataBaseInfo english_read = new DataBaseInfo(
                "jdbc:mysql://drdsbggado8i32w8.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_read
        );
        DataBaseInfo english_oral = new DataBaseInfo(
                "jdbc:mysql://drdsbgga1s358k06.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_oral
        );
        DataBaseInfo english_rank_ebook_gold = new DataBaseInfo(
                "jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_rank_ebook_gold
        );
        DataBaseInfo english_game_etp = new DataBaseInfo(
                "jdbc:mysql://drdsbgga17222szn.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_game_etp
        );

        DataBaseInfo english_basedata_agent_book_parent_skill = new DataBaseInfo(
                "jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_basedata_agent_book_parent_skill
        );

        DataBaseInfo english_student_teache_video = new DataBaseInfo(
                "jdbc:mysql://drdsbgga581301b3.drds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_english_student_teache_video
        );


        DataBaseInfo dwd = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwd
        );

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );


        DataBaseInfo kk = new DataBaseInfo(
                "jdbc:mysql://rm-2ze02m3a090ke3zub.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "LALQJGpfiw1jUyLhTqu6",
                sqlkk
        );

        DataBaseInfo kkProd = new DataBaseInfo(
                "jdbc:mysql://rm-2zeo7cx08119h573m.mysql.rds.aliyuncs.com:3306/",
                "tidb_ro",
                "274AHguQQfgUS98gVseu",
                sql_prod_kk
        );

        DataBaseInfo chi = new DataBaseInfo(
                "jdbc:mysql://tope-qa-all-bj.rwlb.rds.aliyuncs.com:3306/",
                "chin_all_mgr",
                "fBwR3RMFeZ9RyNZC#v",
                sqlchi
        );

        DataBaseInfo chiOldProd = new DataBaseInfo(
                "jdbc:mysql://rm-2ze50a5xi3885jkkc.mysql.rds.aliyuncs.com:3306/",
                "tidb_root",
                "PRb0K_oIV6cIPj2ExsRk",
                sql_prod_old_chi
        );

        DataBaseInfo dim = new DataBaseInfo(
                "jdbc:mysql://172.28.30.28:3306/",
                "zhengyajun",
                "zhengyajun8899",
                sqldim
        );

        DataBaseInfo prodNewChi = new DataBaseInfo(
                "jdbc:mysql://10.16.2.135:40238/",
                "all_data_rw",
                "aFxhbKXx6XQOeXmVxrXv",
                sql_prod_new_chi
        );

        DataBaseInfo prodEngEtp = new DataBaseInfo(
                "jdbc:mysql://drdsbgga1s358k06.drds.aliyuncs.com:3306/",
                "etp_race_rw",
                "1sMmuTrpJCyguHnbDSk8",
                sql_prod_english_etp_race
        );

        DataBaseInfo prodShop = new DataBaseInfo(
                "jdbc:mysql://pc-2zezua03j5nu3l888.mysql.polardb.rds.aliyuncs.com:3306/",
                "pintuan_rw",
                "vQcsXwXu&!nzq37Q",
                sql_prod_shop
        );

        DataBaseInfo prodEnglishAddressbook = new DataBaseInfo(
                "jdbc:mysql://drdsbggasv1o4o86.drds.aliyuncs.com:3306/",
                "addressbook_rw",
                "sBfYETl7ag2o7ySPUZx0",
                sql_prod_english_addressbook
        );

        DataBaseInfo prodUserProduct = new DataBaseInfo(
                "jdbc:mysql://rm-2ze2q2f2dn4o2763v.mysql.rds.aliyuncs.com:3306/",
                "user_root",
                "sZpdOQYQDEU8v5F95To3",
                sql_prod_user_product
        );
        LinkedHashSet<TableInfo> allTabInfo1 = extractTabInfo2(english_read, sql_english_read);
        LinkedHashSet<TableInfo> allTabInfo2 = extractTabInfo2(english_oral, sql_english_oral);
        LinkedHashSet<TableInfo> allTabInfo3_1 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_game;");
        LinkedHashSet<TableInfo> allTabInfo3_2 = extractTabInfoSpecifyDatabase(english_game_etp, sql_english_game_etp, "use english_etp;");
        LinkedHashSet<TableInfo> allTabInfo4_1 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_basedata;");
        LinkedHashSet<TableInfo> allTabInfo4_2 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_agent;");
        LinkedHashSet<TableInfo> allTabInfo4_3 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_book;");
        LinkedHashSet<TableInfo> allTabInfo4_4 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_parent;");
        LinkedHashSet<TableInfo> allTabInfo4_5 = extractTabInfoSpecifyDatabase(english_basedata_agent_book_parent_skill, sql_english_basedata_agent_book_parent_skill, "use english_skill;");
        LinkedHashSet<TableInfo> allTabInfo5_1 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_rank;");
        LinkedHashSet<TableInfo> allTabInfo5_2 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_ebook;");
        LinkedHashSet<TableInfo> allTabInfo5_3 = extractTabInfoSpecifyDatabase(english_rank_ebook_gold, sql_english_rank_ebook_gold, "use english_gold;");
        LinkedHashSet<TableInfo> allTabInfo6_1 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_student;");
        LinkedHashSet<TableInfo> allTabInfo6_2 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_teacher;");
        LinkedHashSet<TableInfo> allTabInfo6_3 = extractTabInfoSpecifyDatabase(english_student_teache_video, sql_english_student_teache_video, "use english_video;");
        LinkedHashSet<TableInfo> allTabInfo7 = extractTabInfo2(chiOldProd, sql_prod_old_chi);
        LinkedHashSet<TableInfo> allTabInfo8 = extractTabInfo2(kkProd, sql_prod_kk);
        LinkedHashSet<TableInfo> allTabInfo9 = extractTabInfo2(prodNewChi, sql_prod_new_chi);
        LinkedHashSet<TableInfo> allTabInfo10 = extractTabInfo2(prodEngEtp, sql_prod_english_etp_race);
        LinkedHashSet<TableInfo> allTabInfo11 = extractTabInfo2(prodShop, sql_prod_shop);
        LinkedHashSet<TableInfo> allTabInfo12 = extractTabInfo2(prodEnglishAddressbook, sql_prod_english_addressbook);
        LinkedHashSet<TableInfo> allTabInfo13 = extractTabInfo2(prodUserProduct, sql_prod_user_product);

        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo1, english_read);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo2, english_oral);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo3_1, english_game_etp);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo3_2, english_game_etp);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo4_1, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo4_2, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo4_3, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo4_4, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo4_5, english_basedata_agent_book_parent_skill);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo5_1, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo5_2, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo5_3, english_rank_ebook_gold);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo6_1, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo6_2, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo6_3, english_student_teache_video);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo7, chiOldProd);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo8, kkProd);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo9, prodNewChi);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo10, prodEngEtp);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo11, prodShop);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo12, prodEnglishAddressbook);
        everyTableSqoopShell_SpecifiedField_allAndIncrement(allTabInfo13, prodUserProduct);

        everyTableOdsAllAndIncrementShell(allTabInfo1);
        everyTableOdsAllAndIncrementShell(allTabInfo2);
        everyTableOdsAllAndIncrementShell(allTabInfo3_1);
        everyTableOdsAllAndIncrementShell(allTabInfo3_2);
        everyTableOdsAllAndIncrementShell(allTabInfo4_1);
        everyTableOdsAllAndIncrementShell(allTabInfo4_2);
        everyTableOdsAllAndIncrementShell(allTabInfo4_3);
        everyTableOdsAllAndIncrementShell(allTabInfo4_4);
        everyTableOdsAllAndIncrementShell(allTabInfo4_5);
        everyTableOdsAllAndIncrementShell(allTabInfo5_1);
        everyTableOdsAllAndIncrementShell(allTabInfo5_2);
        everyTableOdsAllAndIncrementShell(allTabInfo5_3);
        everyTableOdsAllAndIncrementShell(allTabInfo6_1);
        everyTableOdsAllAndIncrementShell(allTabInfo6_2);
        everyTableOdsAllAndIncrementShell(allTabInfo6_3);
        everyTableOdsAllAndIncrementShell(allTabInfo7);
        everyTableOdsAllAndIncrementShell(allTabInfo8);
        everyTableOdsAllAndIncrementShell(allTabInfo9);
        everyTableOdsAllAndIncrementShell(allTabInfo10);
        everyTableOdsAllAndIncrementShell(allTabInfo11);
        everyTableOdsAllAndIncrementShell(allTabInfo12);
        everyTableOdsAllAndIncrementShell(allTabInfo13);


        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();
        allTabInfo.addAll(allTabInfo1);
        allTabInfo.addAll(allTabInfo2);
        allTabInfo.addAll(allTabInfo3_1);
        allTabInfo.addAll(allTabInfo3_2);
        allTabInfo.addAll(allTabInfo4_1);
        allTabInfo.addAll(allTabInfo4_2);
        allTabInfo.addAll(allTabInfo4_3);
        allTabInfo.addAll(allTabInfo4_4);
        allTabInfo.addAll(allTabInfo4_5);
        allTabInfo.addAll(allTabInfo5_1);
        allTabInfo.addAll(allTabInfo5_2);
        allTabInfo.addAll(allTabInfo5_3);
        allTabInfo.addAll(allTabInfo6_1);
        allTabInfo.addAll(allTabInfo6_2);
        allTabInfo.addAll(allTabInfo6_3);
        allTabInfo.addAll(allTabInfo7);
        allTabInfo.addAll(allTabInfo8);
        allTabInfo.addAll(allTabInfo9);
        allTabInfo.addAll(allTabInfo10);
        allTabInfo.addAll(allTabInfo11);
        allTabInfo.addAll(allTabInfo12);
        allTabInfo.addAll(allTabInfo13);

        hiveCreateTable2(allTabInfo);
        azkabanOdsFlow(allTabInfo);
//        removePartitionOdsTable(allTabInfo);
    }


    static void newChi() throws SQLException, IOException {
        String sqlnewchi = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE 'chi%'\n" +
                "OR  a.TABLE_SCHEMA LIKE 'friend_system%'\n" +
                "OR a.TABLE_SCHEMA LIKE 'tope_user_center%';";

        DataBaseInfo newchi1 = new DataBaseInfo(
                "jdbc:mysql://172.28.30.86:3306/",
                "canal",
                "lVHAsgwEn934kQQUWene",
                sqlnewchi
        );
        DataBaseInfo newchi2 = new DataBaseInfo(
                "jdbc:mysql://172.28.30.85:3306/",
                "canal",
                "lVHAsgwEn934kQQUWene",
                sqlnewchi
        );

        DataBaseInfo newchiMaster = new DataBaseInfo(
                "jdbc:mysql://172.28.30.87:3306/",
                "qa_new_chinese",
                "Aa123456",
                sqlnewchi
        );

        LinkedHashSet<TableInfo> allTabInfoTmpAll = new LinkedHashSet<TableInfo>();
        LinkedHashSet<TableInfo> allTabInfotmp1 = extractTabInfo2(newchi1, sqlnewchi);
        LinkedHashSet<TableInfo> allTabInfotmp2 = extractTabInfo2(newchi2, sqlnewchi);
        allTabInfoTmpAll.addAll(allTabInfotmp1);
        allTabInfoTmpAll.addAll(allTabInfotmp2);
        //过滤
        LinkedHashSet<TableInfo> allTabInfo = new LinkedHashSet<TableInfo>();

        for (TableInfo tableInfo : allTabInfoTmpAll) {
            String databaseName = tableInfo.getDatabaseName();
            tableInfo.setDatabaseName(databaseName.substring(0, databaseName.lastIndexOf('_')));
            allTabInfo.add(tableInfo);
        }

//        newChiCreateTable(allTabInfo);
//        hiveCreateTable2(allTabInfo);
        everyTableSqoopShell_SpecifiedField(allTabInfo, newchiMaster);
        everyTableOdsShell(allTabInfo);
        azkabanNewChiFlow(allTabInfo);

    }

    static void fliter_postfix(LinkedHashSet<TableInfo> allTabInfo) {
        for (TableInfo tableInfo : allTabInfo) {
            String databaseName = tableInfo.getDatabaseName();
            databaseName.substring(0, databaseName.length() - 2);
            tableInfo.setDatabaseName(databaseName.substring(0, databaseName.length() - 2));
        }
    }


    static void tmp() throws SQLException, IOException {
        String sqltmp = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_NAME = '" + "agent_relation' \n" +
                "or  a.TABLE_NAME = '" + "lianxu_yuedu20200921' \n" +
                "or  a.TABLE_NAME = '" + "activate_user20201028' ;";

        DataBaseInfo tmp = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqltmp
        );

        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(tmp, sqltmp);


        tmpCreateTable(allTabInfo);
        everyTableSqoopShell_SpecifiedField(allTabInfo, tmp);
        everyTableOdsShell(allTabInfo);
    }


    static void tmpCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("tmp_create_table.sh"));
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
                            + "location \"/bigdata/warehouse/tmp/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";");
            stringBuilder.append("';\n\n");
            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }
//        out.write("';\n");
        out.close();
    }


    //包含建表及初始化及初始化调度，不包含每天的调度
    //shellSqoopLocation最后的目录必须为 layer + _oltp_to_hdfs
    //shellHiveLocation最后的目录必须为 layer + _hdfs_to_hive
    static void prodModule(String sqlmodule, DataBaseInfo module, String layer, String shellSqoopLocation, String shellHiveLocation) throws SQLException, IOException {

        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(module, sqlmodule);
        moduleCreateTable(allTabInfo, layer);
        everyTableSqoopShell_SpecifiedField_module(allTabInfo, module, layer);
        everyTableModuleShell(allTabInfo, layer);
        azkabanModuleInitFlow(allTabInfo, layer, shellSqoopLocation, shellHiveLocation);
    }

    static void prodModule_Dwm() throws SQLException, IOException {
        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );
        prodModule(sqldwm, dwm, "dwm", "/root/bin/azkabanshell/dwm_oltp_to_hdfs", "/root/bin/azkabanshell/dwm_hdfs_to_hive");
    }


    static void prodModule_Dim() throws SQLException, IOException {
        String sqldim = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dim" + "%' ;";

        DataBaseInfo dim = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldim
        );
        prodModule(sqldim, dim, "dim", "/root/bin/azkabanshell/dim_oltp_to_hdfs", "/root/bin/azkabanshell/dim_hdfs_to_hive");
    }

    static void prodModule_Tmp() throws SQLException, IOException {
        String sqltmp = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "tmp" + "%' ;";

        DataBaseInfo tmp = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqltmp
        );
        prodModule(sqltmp, tmp, "tmp", "/root/bin/azkabanshell/tmp_oltp_to_hdfs", "/root/bin/azkabanshell/tmp_hdfs_to_hive");
    }

    static void prodModule_App() throws SQLException, IOException {
        String sqlapp = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "app" + "%' ;";

        DataBaseInfo app = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqlapp
        );
        prodModule(sqlapp, app, "app", "/root/bin/azkabanshell/app_oltp_to_hdfs", "/root/bin/azkabanshell/app_hdfs_to_hive");
    }


    //包含建表及初始化及初始化调度，不包含每天的调度
    static void prodDwm() throws SQLException, IOException {

        //暂时没有加入到逻辑，这两个表示truncate和分区同时存在
        DIMTRUNCATEANDDATAVALUE.add("dwm.dwm_agent_day_buy");
        DIMTRUNCATEANDDATAVALUE.add("dwm.dwm_agent_day");


        String sqldwm = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwm" + "%' ;";

        DataBaseInfo dwm = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwm
        );

        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(dwm, sqldwm);
        dwmCreateTable(allTabInfo);
        everyTableSqoopShell_SpecifiedField_dwm(allTabInfo, dwm);
        everyTableDwmShell(allTabInfo);
//        azkabanDwmInitFlow(allTabInfo);
    }

    static void dwmCreateTable(LinkedHashSet<TableInfo> allTabInfo) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter("dwm_create_table.sh"));
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
                            + "location \"/bigdata/warehouse/dwm/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";");
            stringBuilder.append("';\n\n");
            exceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }
//        out.write("';\n");
        out.close();
    }


    static void moduleCreateTable(LinkedHashSet<TableInfo> allTabInfo, String layer) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(layer + "_create_table.sh"));
        for (TableInfo tableInfo : allTabInfo) {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("hive -n root -p root -e '");
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
                            + "location \"/bigdata/warehouse/" + layer + "/" + tableInfo.getDatabaseName() + "/" + tableInfo.getTableName() + "/\";");
            stringBuilder.append("';\n\n");
            moduleExceptionExitCreateTable(stringBuilder, tableInfo);
            out.write(stringBuilder.toString());
            out.write("\n\n");
        }
//        out.write("';\n");
        out.close();
    }


    public static void fliterSpecialTable(LinkedHashSet<TableInfo> alltables) {
        alltables.removeIf(s -> s.getTableName().equals("__drds__system__recyclebin__"));
    }

    public static void fliterIncrementSpecialTable(LinkedHashSet<TableInfo> alltables) {
//        LinkedHashSet<TableInfo> alltables2 =  new
//        INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName())
        alltables.removeIf(tableInfo -> !INCREMENTTABLE.contains(tableInfo.getDatabaseName() + "." + tableInfo.getTableName()));
    }


    public static void prodDwd() throws SQLException, IOException {

        String sqldwd = "SELECT  a.TABLE_SCHEMA AS dbName , a.TABLE_NAME AS tabName,a.`COLUMN_NAME` AS cluName,a.COLUMN_TYPE AS cluType,a.COLUMN_COMMENT AS columnComment ,b.`TABLE_COMMENT` AS tblComment\n" +
                "FROM INFORMATION_SCHEMA.`COLUMNS`  a\n" +
                "LEFT JOIN INFORMATION_SCHEMA.`TABLES` b\n" +
                "ON a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \n" +
                "WHERE  a.TABLE_SCHEMA LIKE '" + "dwd" + "%' " +
                "and a.TABLE_NAME like 'dwd%' ;";


        DataBaseInfo dwd = new DataBaseInfo(
                "jdbc:mysql://10.16.40.154:3306/",
                "zhuangshuai",
                "0UtLSBLnYajUutJh",
                sqldwd
        );

        LinkedHashSet<TableInfo> allTabInfo = extractTabInfo2(dwd, sqldwd);
        dwdCreateTable(allTabInfo);

        everyTableSqoopShell_SpecifiedField_dwd(allTabInfo, dwd);


        everyTableDwdShell(allTabInfo);

        azkabanDwdInitFlow(allTabInfo);

    }

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
//        chineseRds_to_Tidb();
//        newChi();
//        cdh();
//        tmp();
//        dwn();
//        chineseRds_to_Tidb();

//        prodOds();

//        init();
//        prodOdsAllAndIncrement();



        //
//        prodDwd();
//        prodModule_Dwm();
//        prodModule_Dim();
        prodModule_Tmp();
//        prodModule_App();


    }
}
