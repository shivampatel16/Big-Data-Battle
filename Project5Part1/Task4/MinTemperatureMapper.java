/**
 * Author: Shivam Patel
 * Andrew IDs: shpatel
 * Email: shpatel@cmu.edu
 * Last Modified: December 10, 2022
 * File: MinTemperatureMapper.java
 * Part Of: Project5 Part1 Task4
 *
 * This Java file defines the mapper class for Hadoop Map Reduce program. It
 * maps the various years in the input file as keys and the temperatures of those
 * years as values.
 */

// Defines the package for the class
package edu.cmu.andrew.shpatel;

// Imports necessary for performing IO, Tokenization and Hadoop Map Reduce operations
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    // Defines a final int variable MISSING
    private static final int MISSING = 9999;

    /**
     * Function to perform Hadoop's map operation. It maps the various years in the input
     * file as keys and the temperatures of those years as values.
     * @param key LongWritable key as input to the map function
     * @param value Text as value input to the map function
     * @param output OutputCollector<Text, IntWritable> to form output of map function
     * @param reporter Reporter for the mapper program
     * @throws IOException Exception while performing IO operations
     */
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        // Get line from input file. This was passed in by Hadoop as value.
        // We have no use for the key (file offset) so we are ignoring it.
        String line = value.toString();

        // Get year when weather data was collected. The year is in positions 15-18.
        // This field is at a fixed position within a line.
        String year = line.substring(15, 19);

        // Get the temperature too.
        int airTemperature;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        // Get quality of reading. If not missing and of good quality then
        // produce intermediate (year,temp).
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {

            // for each year in input, reduce will be called with
            // (year,[temp,temp,temp,…])
            // They key is year and the list of temps will be placed in an iterator.
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}
