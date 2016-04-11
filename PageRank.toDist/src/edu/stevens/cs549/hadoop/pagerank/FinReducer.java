package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	public ArrayList<String> names = new ArrayList<String>(10);

	public void setup(Context context) throws IOException {
		URI[] files = context.getCacheFiles();
		Path path = new Path(files[0]);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(files[0], conf);

		FileStatus[] ls = fs.listStatus(path);
		for (FileStatus file : ls) { // Modified file to support multiple
										// reducer output files.
			if (file.getPath().getName().startsWith("names")) {
				FSDataInputStream readin = fs.open(file.getPath());
				BufferedReader d = new BufferedReader(new InputStreamReader(readin));

				String name = d.readLine();
				while (name != null) {
					System.out.println("Add Name: "+name);
					names.add(name);
					name = d.readLine();

				}
				d.close();
			}
		}

		return;
	}

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 *  For each value, emit: key:value, value:-rank
		 */
		for (Text v : values) {
			String curName=names.get(Integer.parseInt(v.toString())-1);
			System.out.println("Vertex id:"+v.toString());
			System.out.println("Finally Get Name: "+curName);
			context.write(new Text(v+" "+curName), new Text(Double.toString(Double.parseDouble(key.toString()))));
		}
	}
}
