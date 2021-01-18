package com.tpln;

/**
 * @author Shuai
 * @create 2021-01-15 10:22
 */
public class ShardingSqoop {

    public static void main(String[] args) {
        System.out.println("a");


        //需要的类A=>{
        //库 表 list<filedInfo>,
        //

        // }

        //1.从hive取出表结构和库表名，并去掉前缀
        // 返回类型：list<{库，表, list<{字段类}>}>

        //2.手动列出得到，drds链接和库的映射
        // 返回类型：map<库，drds链接类>

        //3.得到库表对应的drds链接
        // 返回类型：list<{{库，表, list<{字段类}>},drds链接类}> =>(group by key(链接)) list<{drds链接，list<{库，表, list<{字段类}>}>}>

        //4.对每个库表 show topology from 库表
        // 返回类型：list<{库，表，list<{字段类}>，list<{分库，分表}>}>

        //5.从每个rds取出分库和分表
        // 返回类型：list<{rds链接类,分库，分表}> => map<{分库，分表}，rds链接类>

        //6.对从4得到的结构使用5添加链接值
        // 返回类型：list<{库，表，list<{字段类}>，list<{分库，分表，rds链接类}>}> =>(group by key(分库名)) list<{库，表，list<{分库，list<{分表，rds链接类}>}>}>




    }

}
