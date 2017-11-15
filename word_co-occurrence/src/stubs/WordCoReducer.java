package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			// calculate the count of the word co-occurrence
			for (IntWritable value: values){
				count += value.get();    	
		    }
		    context.write(key,new IntWritable(count));
	    }
}
