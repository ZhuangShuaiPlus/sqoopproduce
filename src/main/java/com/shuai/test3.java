package com.shuai;

import java.io.File;
import java.util.LinkedHashMap;

/**
 * @author Shuai
 * @create 2020-10-09 15:46
 */


public class test3 {
    public static void main(String[] args) {
        LinkedHashMap<TableInfo, String> stringStringLinkedHashMap = new LinkedHashMap<TableInfo, String>();

//        TableInfo tableInfo = new TableInfo();
//        tableInfo.databaseName="aaa";
//        tableInfo.tableName="bbb";
//        tableInfo.tableComment = "shuai";
//
//
//        TableInfo tableInfo2 = new TableInfo();
//        tableInfo2.databaseName="aaa2";
//        tableInfo2.tableName="bbb2";
//        tableInfo2.tableComment = "shuai";
//
//        TableInfo tableInfo3 = new TableInfo();
//        tableInfo3.databaseName="aaa2";
//        tableInfo3.tableName="bbb2";
//        tableInfo3.tableComment = "shuai";


//        System.out.println(tableInfo.equals(tableInfo2));
//        System.out.println(tableInfo.equals(tableInfo3));
//        System.out.println(tableInfo2.equals(tableInfo3));
//
//        stringStringLinkedHashMap.put(tableInfo,"aa");
//        stringStringLinkedHashMap.put(tableInfo2,"aa");
//        stringStringLinkedHashMap.put(tableInfo3,"aa");
//
//        System.out.println(stringStringLinkedHashMap.size());
        String fileLocation = "./english_agent/";
        System.out.println(fileLocation);
        File f = new File(fileLocation);
        if (!f.exists()) {
            f.mkdirs();
        }
    }
}
