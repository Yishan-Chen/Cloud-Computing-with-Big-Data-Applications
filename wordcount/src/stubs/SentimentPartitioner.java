package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable>
		implements Configurable {

	private Configuration configuration;

	// Create two hashsets to store the positive words and negative words from
	// the input files
	Set<String> positive = new HashSet<String>();
	Set<String> negative = new HashSet<String>();

	// Read files from Distributed cache
	File positiveFile = new File("positive-words.txt");
	File negativeFile = new File("negative-words.txt");

	/**
	 * Read words from file and write it into the hashset
	 * 
	 * @throws IOException
	 */
	public HashSet<String> readFile(File file) throws IOException {
		HashSet<String> container = new HashSet<String>();
		Scanner in = new Scanner(file);
		String line = null;
		while(in.hasNext()){  // check if the content hits the end
			line = in.next();
			if(line.startsWith(";")){continue;}
			container.add(line);
		}
		in.close();
		return container;
	}

	@Override
	public void setConf(Configuration configuration) {
		try {
			// read the content of positive files and negative files into hashset
			positive = readFile(positiveFile);
			negative = readFile(negativeFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Implement the getConf method for the Configurable interface.
	 */
	@Override
	public Configuration getConf() {
		return configuration;
	}

	/**
	 * You need to implement the getPartition method for a partitioner class.
	 * This method receives the words as keys (i.e., the output key from the
	 * mapper.) It should return an integer representation of the sentiment
	 * category (positive, negative, neutral).
	 * 
	 * For this partitioner to work, the job configuration must have been set so
	 * that there are exactly 3 reducers.
	 */
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		/*
		 * TODO implement Change the return 0 statement below to return the
		 * number of the sentiment category; use 0 for positive words, 1 for
		 * negative words, and 2 for neutral words. Use the sets of positive and
		 * negative words to find out the sentiment.
		 * 
		 * Hint: use positive.contains(key.toString()) and
		 * negative.contains(key.toString()) If a word appears in both lists
		 * assume it is positive. That is, once you found that a word is in the
		 * positive list you do not need to check if it is in the negative list.
		 */
		if (positive.contains(key.toString())) {
			return 0;
		} else if (negative.contains(key.toString())) {
			return 1;
		} else {
			return 2;
		}
	}
}
