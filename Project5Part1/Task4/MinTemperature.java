/**
 * Author: Shivam Patel
 * Andrew IDs: shpatel
 * Email: shpatel@cmu.edu
 * Last Modified: December 10, 2022
 * File: MinTemperature.java
 * Part Of: Project5 Part1 Task4
 *
 * This Java file defines a combiner class for Hadoop Map Reduce program. It
 * calls the main method which sets the mapper and reducer class for the
 * program.
 */

// Defines the package for the class
package edu.cmu.andrew.shpatel;

// Imports necessary for performing IO, Tokenization and Hadoop Map Reduce operations
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import java.io.*;

public class MinTemperature {
    public static void main(String[] args) throws IOException {

        // If two arguments are not given to the main method
        if (args.length != 2) {

            // Display error to the user
            System.err.println("Usage: MinTemperature <input path> <output path>");

            // Exit program
            System.exit(-1);
        }

        // Create a new job configuration object
        JobConf conf = new JobConf(MinTemperature.class);

        // Set job configuration name
        conf.setJobName("Min temperature");

        // Set input file path
        FileInputFormat.addInputPath(conf, new Path(args[0]));

        // Set output file path
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Set mapper class
        conf.setMapperClass(MinTemperatureMapper.class);

        // Set reducer class
        conf.setReducerClass(MinTemperatureReducer.class);

        // Set output key's class
        conf.setOutputKeyClass(Text.class);

        // Set output value's class
        conf.setOutputValueClass(IntWritable.class);

        // Run job
        JobClient.runJob(conf);
    }
}
