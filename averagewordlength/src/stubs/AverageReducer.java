package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	
	int wordCount = 0;
	double wordLength = 0;
    double averageLength = 0;
    
    for (IntWritable value: values){
    	
		wordCount ++;
    	wordLength += value.get();    	
    }
    averageLength = wordLength / wordCount;
    context.write(key,new DoubleWritable(averageLength));

  }
}