/**
 * Author: Shivam Patel
 * Andrew IDs: shpatel
 * Emails: shpatel@cmu.edu
 * Last Modified: December 10, 2022
 * File: TempestAnalytics.java
 * Part Of: Project5Part2
 *
 * This Java file uses Spark to read TheTempest.txt file and perform various
 * calculations on its contents. The location of the file is given as arguments
 * to the main() function. The calculations performed on TheTempest.txt file are:
 *
 * - Calculate total number of lines in the file
 * - Calculate total number of words in the file
 * - Calculate total number of distinct words in the file
 * - Calculate total number of symbols in the file
 * - Calculate total number of distinct symbols in the file
 * - Calculate total number of distinct letters in the file
 * - Request the user for an input word and print all the lines
 *   that contain the word entered by the user
 */

// Imports necessary for executing Spark, use Arrays and Scanner classes
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;
import java.util.Scanner;

public class TempestAnalytics {

    public static void main(String[] args) {

        // Create a Spark configuration object, setting the master to local and the app name to Project 5 Part 2
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Project 5 Part 2");

        // Create a JavaSparkContext object with the required spark configurations
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Create an RDD that represents the data file
        JavaRDD<String> inputFile = sparkContext.textFile(args[0]);

        // Create an RDD built up from each line split into words
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+")));

        // ##### Project 5 - Part 2 - Task 0 #####
        // Counts the number of lines in the input file
        long numberOfLines = inputFile.count();

        // Displays number of lines in the input file to the user
        System.out.println("Number of lines: " + numberOfLines);

        // ##### Project 5 - Part 2 - Task 1 #####
        // Counts the number of words in the input file
        long numberOfWords = wordsFromFile.filter(k -> ( !k.isEmpty())).count();

        // Displays number of words in the input file to the user
        System.out.println("Number of words: " + numberOfWords);

        // ##### Project 5 - Part 2 - Task 2 #####
        // Counts the number of distinct words in the input file
        long numberOfDistinctWords = wordsFromFile.filter(k -> ( !k.isEmpty())).distinct().count();

        // Displays number of distinct words in the input file to the user
        System.out.println("Number of distinct words: " + numberOfDistinctWords);

        // ##### Project 5 - Part 2 - Task 3 #####
        // Create an RDD built up from each line split into symbols
        JavaRDD<String> symbolsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("")));

        // Counts the number of symbols in the input file
        long numberOfSymbols = symbolsFromFile.count();

        // Displays number of symbols in the input file to the user
        System.out.println("Number of symbols: " + numberOfSymbols);

        // ##### Project 5 - Part 2 - Task 4 #####
        // Counts the number of distinct symbols in the input file
        long numberOfDistinctSymbols = symbolsFromFile.distinct().count();

        // Displays number of distinct symbols in the input file to the user
        System.out.println("Number of distinct symbols: " + numberOfDistinctSymbols);

        // ##### Project 5 - Part 2 - Task 5 #####
        // Counts the number of distinct letters in the input file
        long numberOfDistinctLetters = symbolsFromFile.filter(k -> (k.matches("[a-zA-Z]"))).distinct().count();

        // Displays number of distinct letters in the input file to the user
        System.out.println("Number of distinct letters: " + numberOfDistinctLetters);

        // ##### Project 5 - Part 2 - Task 6 #####
        // Create an object of Scanner class to get input from the user
        Scanner s = new Scanner(System.in);

        // Function to print line containing the word entered by user to the output screen
        VoidFunction<String> printLineContainingUserWord = line -> System.out.println(line);

        while (true) {
            // Prompt the user for input
            System.out.println("\nEnter a word to search in TheTempest.txt (Enter \"#$@Done@$#\" to stop): ");

            // Get input from user
            String userWord = s.next();

            // If the user enters "#$@Done@$#", stop the execution of while loop
            // Kept the word to be "#$@Done@$#", as that would not be present in the input file
            if (userWord.equals("#$@Done@$#")) {
                break;
            }

            // Create an RDD built up from each line in the input file split by a new line character
            JavaRDD<String> linesFromFile = inputFile.flatMap(content -> Arrays.asList(content.split("\n")));

            // Update RDD to contain only the lines that contain the word entered by the user
            linesFromFile = linesFromFile.filter(k -> (k.contains(userWord)));

            // Loop over the RDD and print lines containing the input word to the console
            linesFromFile.foreach(printLineContainingUserWord);
        }
        // Exit message
        System.out.println("\nQuitting! Thank you :)");
    }
}
