package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		String line = value.toString(); // Converts Line to a String
		String[] res = line.split(":");
		/* 
		 * Just echo the input, since it is already in adjacency list format.
		 */
	


        context.write(new Text(res[0]), new Text(res[1]));

	}

}
