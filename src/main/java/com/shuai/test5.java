package com.shuai;

public class test5 {
    public static void main(String[] args) {
//        String s = "a-a";
//        s = standardization(s);
//        System.out.println(s);

        String shuai = "shuai";
//

        System.out.println();
        System.out.println(shuai.substring(0,shuai.lastIndexOf('a')));

    }

    static String fliterLine(String s) {
        s = s.replaceAll("-", "_");
        return s;
    }

    static String standardization(String s) {
        return fliterLine(s);
    }

}
