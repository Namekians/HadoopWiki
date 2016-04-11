package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part
												// 1: node+rank | Part 2: adj
												// list

		if (sections.length > 2) // Checks if the data is in the incorrect
									// format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}

		/*
		 * emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node! Put a
		 * marker on the string value to indicate it is an adjacency list.
		 */
		String node = sections[0].split("\\+")[0];
		double rank = Double.parseDouble(sections[0].split("\\+")[1]);
		String temp = sections[1];
		temp = temp.replaceAll("\\s+", "");
		temp = temp.replaceAll("\\t+", "");
		temp = temp.replaceAll("\\*+", "");
		String[] vertex = temp.split("");

		for (String v : vertex) {
			System.out.println("Mapper emit key:" + v + "End");
			context.write(new Text(v), new Text(Double.toString((rank / vertex.length))));

		}
		System.out.println("Mapper emit list:" + node + "End");
		context.write(new Text(node), new Text("*" + sections[1]));
	}

}
