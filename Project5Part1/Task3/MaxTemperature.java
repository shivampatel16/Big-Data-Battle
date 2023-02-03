/**
 * Author: Shivam Patel
 * Andrew IDs: shpatel
 * Email: shpatel@cmu.edu
 * Last Modified: December 10, 2022
 * File: MaxTemperature.java
 * Part Of: Project5 Part1 Task3
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

public class MaxTemperature {
    public static void main(String[] args) throws IOException {

        // If two arguments are not given to the main method
        if (args.length != 2) {

            // Display error to the user
            System.err.println("Usage: MaxTemperature <input path> <output path>");

            // Exit program
            System.exit(-1);
        }

        // Create a new job configuration object
        JobConf conf = new JobConf(MaxTemperature.class);

        // Set job configuration name
        conf.setJobName("Max temperature");

        // Set input file path
        FileInputFormat.addInputPath(conf, new Path(args[0]));

        // Set output file path
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Set mapper class
        conf.setMapperClass(MaxTemperatureMapper.class);

        // Set reducer class
        conf.setReducerClass(MaxTemperatureReducer.class);

        // Set output key's class
        conf.setOutputKeyClass(Text.class);

        // Set output value's class
        conf.setOutputValueClass(IntWritable.class);

        // Run job
        JobClient.runJob(conf);
    }
}

