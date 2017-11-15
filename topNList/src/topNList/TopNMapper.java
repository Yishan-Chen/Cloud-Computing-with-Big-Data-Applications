package topNList;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	private int N = 3; // default
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>(); //sortedMap to store top n movies
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String input = value.toString().trim();	//read input from values and convert to specify type
		String[] tokens = input.split("\\W+");
		String movieID = tokens[0];
		int ranting = Integer.parseInt(tokens[1]);
		
		String composite = movieID + "," + ranting;
		top.put(ranting, composite);

		if (top.size() > N) {				//remove the movies with lowest ranting when top.size larger than N
			top.remove(top.firstKey());
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.N = context.getConfiguration().getInt("N", 3); // get N from configuration, and set N default to 3
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String str : top.values()) {					// emit (_,(movieID, ranting))
			context.write(NullWritable.get(), new Text(str));
		}
	}
}
