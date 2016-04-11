package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		System.out.println("Reducer:"+key.toString());
		String list ="";
		Double res = (double) 0;
	
		for(Text t:values){
			if(t.toString().charAt(0)=='*'){
				list = t.toString();
				continue;
			}
			System.out.println("Reducer:"+t.toString());

			res+=Double.parseDouble(t.toString());
		}
		
		res=1-d+d*res;
		context.write(new Text(key.toString()+'+'+Double.toString(res)),new Text(list) );
			
	}
}
