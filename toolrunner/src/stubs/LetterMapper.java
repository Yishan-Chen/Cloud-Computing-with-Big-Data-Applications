package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	//create object once and reuse it in all mapper 
	private Text wordObject = new Text();
	private boolean caseSenstive;
	
  /**
   * set up the job configuration before the mapper work
   */
  public void setup(Context context){
	  Configuration conf = context.getConfiguration();
	  caseSenstive = conf.getBoolean("caseSenstive", true);
  }	
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  // read the value 
	  String line = value.toString();
	  // read every single word using the regular expression
	  for (String word : line.split("\\W+")){  
		  if(word.length() > 0){
			  // check if it required to be caseSenstive then convert all the word to lower case
			  if (caseSenstive == true){
				  word = word.toLowerCase();
			  }
			  // Extract the first letter of the word
			  wordObject.set(word.substring(0,1));
			  context.write(wordObject,new IntWritable(word.length()));
		  }
	  }

  }
}
