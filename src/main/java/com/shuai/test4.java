package com.shuai;

import java.util.LinkedHashSet;

public class test4 {
    public static void main(String[] args) {
        LinkedHashSet<String> strings = new LinkedHashSet<>();
        LinkedHashSet<String> strings2 = new LinkedHashSet<>();
        strings.add("a");
        strings.add("b");
        strings2.add("c");
        strings2.add("d");

        System.out.println(strings);
        strings.addAll(strings2);

        System.out.println(strings);

    }
}
