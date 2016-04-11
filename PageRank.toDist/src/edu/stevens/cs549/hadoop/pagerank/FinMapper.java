package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
//	@Override
//	public void setup(Context context) throws IOException{
//		super(context);
//		URI[] files= context.getCacheFiles();
//		Path path= new Path(files[0]);
//		}
//	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */

		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list
		String node=sections[0].split("\\+")[0];
	    Double rank= Double.parseDouble(sections[0].split("\\+")[1]);
		context.write(new DoubleWritable(-rank), new Text(node));
	}

}
