package com.tpln;

public class test1 {
    public static void main(String[] args) {
        System.out.println("aaa");

        String host = "jdbc:mysql://drdsbggasxc095l0.drds.aliyuncs.com:3306/".split("/")[2].split(":")[0];
        System.out.println(host);
    }
}
