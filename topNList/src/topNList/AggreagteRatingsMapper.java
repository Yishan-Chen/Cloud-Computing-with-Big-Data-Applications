package topNList;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggreagteRatingsMapper extends Mapper<Object, Text, LongWritable, IntWritable>{
	
	LongWritable k = new LongWritable();
	IntWritable v = new IntWritable();
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String input = value.toString().trim();			// read values and convert to specify type
		String[] tokens = input.split(",");
		long movieID = Long.parseLong(tokens[0]);
		int ranting = (int)Double.parseDouble(tokens[2]);	
		k.set(movieID);
		v.set(ranting);
		context.write(k, v);							// emit (movieID,ranting) pair
	}
}
