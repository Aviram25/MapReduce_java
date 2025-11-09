package wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Basic Word Count MapReduce application
 * Counts the frequency of each word in input text files
 * This is the simplest implementation without combiners or custom partitioners
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Mapper class that tokenizes input text lines and emits word-count pairs
	 * Input: Key = line offset (LongWritable), Value = line of text (Text)
	 * Output: Key = word (Text), Value = 1 (IntWritable)
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// Constant value of 1 - reused across all map calls for efficiency
		// Avoids creating new IntWritable objects for each word
		private static final IntWritable ONE = new IntWritable(1);
		
		// Reusable Text object for output keys
		// Reduces garbage collection overhead
		private Text outputKey = new Text();
		
		/**
		 * Map function that processes each input line
		 * Splits the line into words and emits each word with a count of 1
		 * 
		 * @param key Line number/offset in the input file (not used here)
		 * @param value The actual line of text to process
		 * @param context Context object for writing output key-value pairs
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Convert Text object to String for processing
			String currentLine = value.toString();
			
			// Split the line by spaces to extract individual words
			String [] words = StringUtils.split(currentLine, ' ');
			
			// Emit each word as a key with value 1
			// Multiple occurrences of the same word will be grouped by the framework
			for(String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
	}
	
	/**
	 * Reducer class that aggregates counts for each word
	 * Input: Key = word (Text), Values = list of counts (Iterable<IntWritable>)
	 * Output: Key = word (Text), Value = total count (IntWritable)
	 * 
	 * The MapReduce framework automatically groups all values with the same key
	 * and passes them to the reducer, so this reducer receives all counts for each word
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Reusable IntWritable for output values
		private IntWritable outputValue = new IntWritable();
		
		/**
		 * Reduce function that sums all counts for a given word
		 * Called once for each unique word in the input
		 * 
		 * @param key The word being counted
		 * @param values Iterator over all count values for this word (typically all 1s)
		 * @param context Context object for writing output key-value pairs
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			// Sum all the counts for this word
			// If no combiner is used, these will all be 1s
			for(IntWritable count : values) {
				sum += count.get();
			}
			
			// Emit the word with its total count
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Configures and executes the MapReduce job
	 * Sets up input/output paths, mapper/reducer classes, and data types
	 * 
	 * @param args Command line arguments:
	 *             args[0] = input path (file or directory containing text files)
	 *             args[1] = output path (directory where results will be written)
	 * @return 0 if job completes successfully, 1 if it fails
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create a new MapReduce job instance with a descriptive name
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		
		// Set the jar class - tells Hadoop where to find the job's classes
		job.setJarByClass(getClass());
		
		// Parse input and output paths from command line arguments
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		// Delete output directory if it already exists
		// MapReduce jobs fail if the output directory exists beforehand
		out.getFileSystem(conf).delete(out, true);
		
		// Set input and output paths for the job
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Configure the Mapper and Reducer classes
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Set input and output format classes
		// TextInputFormat reads files line by line
		// TextOutputFormat writes output as text files
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set the output types for mapper (intermediate data between map and reduce)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set the output types for reducer (final output)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Submit the job and wait for completion
		// Returns true if successful, false otherwise
		// Converts boolean to integer: true->0 (success), false->1 (failure)
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * Main entry point for the Word Count application
	 * 
	 * @param args Command line arguments passed to the run() method
	 * 
	 * Usage:
	 * hadoop jar wordcount.jar /path/to/input.txt /path/to/output
	 * or
	 * yarn jar wordcount.jar /user/hadoop/books /user/hadoop/wordcount_results
	 * 
	 * Output format (in part-r-00000 file):
	 * word1	count1
	 * word2	count2
	 * word3	count3
	 * ...
	 * 
	 * Note: Output is tab-delimited by default
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			// ToolRunner provides support for generic Hadoop command-line options
			// like -conf, -D, -fs, -jt, etc.
			result = ToolRunner.run(new Configuration(), 
							new WordCountJob(),
							args);
		} catch (Exception e) {
			// Print stack trace if any exception occurs during execution
			e.printStackTrace();
		}
		// Exit with the job's return code
		// 0 indicates success, 1 indicates failure
		System.exit(result);
	}
}
