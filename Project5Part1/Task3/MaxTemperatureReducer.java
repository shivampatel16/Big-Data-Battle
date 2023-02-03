/**
 * Author: Shivam Patel
 * Andrew IDs: shpatel
 * Email: shpatel@cmu.edu
 * Last Modified: December 10, 2022
 * File: MaxTemperatureReducer.java
 * Part Of: Project5 Part1 Task3
 *
 * This Java file defines the reducer class for Hadoop Map Reduce program. It
 * reduces the various temperatures give for a particular year to find the maximum
 * temperature for that year.
 */

// Defines the package for the class
package edu.cmu.andrew.shpatel;

// Imports necessary for performing IO, Tokenization and Hadoop Map Reduce operations
import java.io.IOException; import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer; import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Function to perform Hadoop's reduce operation. It gets the key:value pairs from the
     * mapper function, reduces the output from mapper to find the maximum temperature for
     * each year in the input file
     * @param key Text as input key to the reduce function
     * @param values Iterable<Text> as values to the reduce function
     * @param output Context to form output of reduce function
     * @param reporter Reporter for the reducer program
     * @throws IOException Exception while performing IO operations
     */
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,
            IntWritable> output, Reporter reporter) throws IOException {

        // Initialize the maxvalue to the minimum possible value of an integer
        int maxValue = Integer.MIN_VALUE;

        // from the list of values, find the maximum
        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }

        // emit (key = year, value = maxTemp = max for year)
        output.collect(key, new IntWritable(maxValue));
    }
}
