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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Classic Word Count MapReduce application
 * Counts the frequency of each word in input text files
 * Demonstrates the use of Mapper, Combiner, and Reducer
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Mapper class that tokenizes input text and emits word-count pairs
	 * Input: Line number (offset), Line of text
	 * Output: Key = word (Text), Value = 1 (IntWritable)
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// Constant value of 1 - reused for efficiency (avoids creating new objects)
		private static final IntWritable ONE = new IntWritable(1);
		
		// Reusable Text object for output keys
		private Text outputKey = new Text();
		
		/**
		 * Map function that processes each line of input text
		 * Splits line into words and emits each word with count of 1
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Convert Text object to String
			String currentLine = value.toString();
			
			// Split the line by spaces into individual words
			String [] words = StringUtils.split(currentLine, ' ');
			
			// Emit each word with a count of 1
			for(String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
	}
	
	/**
	 * Combiner class that performs local aggregation on mapper output
	 * Reduces the amount of data transferred over the network to reducers
	 * Input: Key = word, Values = list of counts (all 1s from mapper)
	 * Output: Key = word, Value = local sum of counts
	 * 
	 * Note: Combiners are optional but improve performance significantly
	 * They act as "mini-reducers" that run on the same node as mappers
	 */
	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Reusable IntWritable for output values
		private IntWritable outputValue = new IntWritable();
		
		/**
		 * Combine function that sums up counts for each word locally
		 * Reduces network traffic by combining multiple (word, 1) pairs into (word, sum)
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			
			// Sum all the counts for this word from local mapper output
			for (IntWritable count : values) {
				sum += count.get();
			}
			
			// Emit the word with its local sum
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Reducer class that computes the final word counts
	 * Input: Key = word, Values = list of counts (from mappers or combiners)
	 * Output: Key = word, Value = total count across all input
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Reusable IntWritable for output values
		private IntWritable outputValue = new IntWritable();
		
		/**
		 * Reduce function that calculates the final count for each word
		 * Aggregates all counts (possibly already partially summed by combiners)
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			// Sum all counts for this word across all mapper/combiner outputs
			for(IntWritable count : values) {
				sum += count.get();
			}
			
			// Emit the word with its final total count
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Configures and runs the MapReduce job
	 * @param args Command line arguments:
	 *             args[0] = input path (file or directory)
	 *             args[1] = output path (directory - must not exist)
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create new MapReduce job with a descriptive name
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		
		// Set the jar class (used to locate the jar file for job submission)
		job.setJarByClass(getClass());
		
		// Set up input and output paths from command-line arguments
		Path in = new Path(args[0]);   // Input file/directory path
		Path out = new Path(args[1]);  // Output directory path
		
		// Delete output directory if it already exists to avoid conflicts
		out.getFileSystem(conf).delete(out, true);
		
		// Configure input and output paths for the job
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Set the Mapper, Reducer, and Combiner classes
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountCombiner.class);  // Combiner improves performance
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);   // Read text files line by line
		job.setOutputFormatClass(TextOutputFormat.class); // Write output as text
		
		// Set output types for mapper (intermediate data)
		job.setMapOutputKeyClass(Text.class);        // Mapper output key type
		job.setMapOutputValueClass(IntWritable.class); // Mapper output value type
		
		// Set output types for reducer (final output)
		job.setOutputKeyClass(Text.class);           // Final output key type (word)
		job.setOutputValueClass(IntWritable.class);  // Final output value type (count)
		
		// Submit the job and wait for completion
		// Returns 0 if successful, 1 if failed
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * Main entry point for the Word Count application
	 * @param args Command line arguments: [input_path] [output_path]
	 * 
	 * Example usage:
	 * hadoop jar wordcount.jar input.txt output
	 * or
	 * yarn jar wordcount.jar /user/input/books /user/output/wordcount
	 * 
	 * Output format:
	 * word1    count1
	 * word2    count2
	 * ...
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			// ToolRunner handles common Hadoop command-line options
			result = ToolRunner.run(new Configuration(), 
							new WordCountJob(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with the job's return code (0 = success, 1 = failure)
		System.exit(result);
	}
}
