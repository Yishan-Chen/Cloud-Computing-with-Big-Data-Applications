package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.junit.Test;


public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() {

		mpart=new SentimentPartitioner();
		mpart.setConf(new Configuration());
		int result;		
		
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
		//Create key value pair to test the partitioner
        Text love = new Text("love");
        Text deadly = new Text("deadly");
        Text zodiac = new Text("zodiac");
        
        IntWritable value = new IntWritable(1);
        
        // use asset equal to test the input
        assertEquals(mpart.getPartition(love,value,3), 0);
        assertEquals(mpart.getPartition(deadly,value,3), 1);        
        assertEquals(mpart.getPartition(zodiac,value,3), 2);
		
	}

}
