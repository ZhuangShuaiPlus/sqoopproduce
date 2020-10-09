package com.shuai;

import java.util.*;

public class test1 {
    public static void main(String[] args) {

        Map<String, String> hashMap = new LinkedHashMap<String, String>();
        hashMap.put("name1", "value1");
        hashMap.put("name2", "value2");
        hashMap.put("name3", "value3");
        Set<Map.Entry<String, String>> set = hashMap.entrySet();
        Iterator<Map.Entry<String, String>> iterator = set.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            System.out.println("key:" + key + ", value:" + value);
        }



//        dbName|tabName|COLUMN_NAME|COLUMN_TYPE
//        chinese_introduction|search_history|searchContent|varchar(32)
//        chinese_introduction|article_article_info|articleTitle|varchar(128)
//        chinese_parent|parent_read_dubbing_msg|id|bigint(20)
//        chinese_parent|day_study_data_stat_copy|isValid|tinyint(4)


        LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> stringLinkedHashMapLinkedHashMap = new LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>>();



        ArrayList<String> strings = new ArrayList<String>();


//        stringLinkedHashMapLinkedHashMap.put()
    }


}
