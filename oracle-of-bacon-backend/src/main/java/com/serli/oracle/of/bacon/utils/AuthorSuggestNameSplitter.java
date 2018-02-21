package com.serli.oracle.of.bacon.utils;

import java.util.*;
import java.util.stream.Collectors;

public class AuthorSuggestNameSplitter {

/*
    Récupéré depuis le GitHub de
     * Steeve SINAGAGLIA
     * Damien RENAUD
*/

    public static String[] createSuggestArray(String authorName) {

        authorName = authorName.replace(",", "");
        String[] c = authorName.split(" ");
        Set<String> result = new HashSet<>();
        return permutation(c, 0, result);
    }

    private static void swap(int pos1, int pos2, String[] c) {
        String temp = c[pos1];
        c[pos1] = c[pos2];
        c[pos2] = temp;
    }

    private static String[] permutation(String[] c, int start, Set<String> res) {

        if (start != 0) {
            String row = "";
            for (int i = 0; i < start; i++) {
                row += c[i] + " ";
            }
            res.add(row.trim());
        }


        for (int i = start; i < c.length; i++) {
            swap(start, i, c);
            permutation(c, start + 1, res);
            swap(start, i, c);
        }

        String[] finalArray = new String[res.size()];

        int counter = 0;
        for (String re : res) {
            finalArray[counter] = re;
            counter++;
        }

        return finalArray;
    }


    /**
     * https://stackoverflow.com/questions/5162254/all-possible-combinations-of-an-array
     * Takes from superfav on stackoverflow and made improvement.
     *
     * @param authorName the name of the author to split
     * @return a list of all linear combinations
     */
    public static List<String> getAllCombinations(String authorName) {
        authorName = authorName.replace(",", "");
        String[] stringsToSplit = authorName.split(" ");

        List<List<String>> powerSet = new LinkedList<List<String>>();

        for (int i = 1; i <= stringsToSplit.length; i++)
            powerSet.addAll(combination(Arrays.asList(stringsToSplit), i));

        return powerSet.stream().map(e -> String.join(" ", e)).collect(Collectors.toList());
    }

    /**
     * https://stackoverflow.com/questions/5162254/all-possible-combinations-of-an-array
     * Takes from superfav on stackoverflow.
     */
    private static <T> List<List<T>> combination(List<T> values, int size) {

        if (0 == size) {
            return Collections.singletonList(Collections.<T>emptyList());
        }

        if (values.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<T>> combination = new LinkedList<List<T>>();

        T actual = values.iterator().next();

        List<T> subSet = new LinkedList<T>(values);
        subSet.remove(actual);

        List<List<T>> subSetCombination = combination(subSet, size - 1);

        for (List<T> set : subSetCombination) {
            List<T> newSet = new LinkedList<T>(set);
            newSet.add(0, actual);
            combination.add(newSet);
        }

        combination.addAll(combination(subSet, size));

        return combination;
    }
}