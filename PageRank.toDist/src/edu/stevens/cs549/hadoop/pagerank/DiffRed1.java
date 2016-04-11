package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		int count=0;
		/* 
		 * The list of values should contain two ranks.  Compute and output their difference.
		 */
		Double difference;
		
        for(Text v:values){
        	ranks[count]=Double.parseDouble(v.toString());
        	count++;
        }
		  difference =Math.abs(ranks[0]-ranks[1]);
		context.write(new Text(), new Text(Double.toString(difference)));
	}
}
