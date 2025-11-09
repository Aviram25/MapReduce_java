package wordcount;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
 * Word Count MapReduce application with custom partitioning and logging
 * Demonstrates use of custom Partitioner to control data distribution across reducers
 * Includes logging for debugging mapper lifecycle methods
 */
public class WordCountJob extends Configured implements Tool {
	
	/**
	 * Custom Partitioner that distributes words to reducers based on word length and count
	 * Partitioners determine which reducer receives a particular key-value pair
	 * This allows control over load balancing and output file organization
	 */
	public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {
		
		/**
		 * Determines which reducer (partition) should receive the given key-value pair
		 * @param key The word (Text)
		 * @param value The count (IntWritable) 
		 * @param numReduceTasks Total number of reducer tasks
		 * @return Partition number (0 to numReduceTasks-1)
		 * 
		 * Partitioning strategy: (word_length * count) % numReduceTasks
		 * This distributes words based on their length and count value
		 */
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			System.out.println("Inside Custom Partitioner Class");
			
			// If only one reducer, all data goes to partition 0
			if (numReduceTasks == 1) {
				return 0;
			}
			
			// Custom partitioning logic: use word length multiplied by count value
			// Modulo ensures the result is within valid partition range [0, numReduceTasks-1]
			return (key.toString().length() * value.get()) % numReduceTasks;
		}
	}
	
	/**
	 * Mapper class that tokenizes input text and emits word-count pairs
	 * Includes logging to track mapper lifecycle: setup → map (multiple times) → cleanup
	 * Input: Line offset (LongWritable), Line of text (Text)
	 * Output: Key = word (Text), Value = 1 (IntWritable)
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		// Logger for debugging mapper execution
		// Useful for tracking mapper lifecycle and troubleshooting issues
		private Logger logger = Logger.getLogger(WordCountMapper.class);
		
		/**
		 * Setup method called ONCE before any map() calls
		 * Used for initialization tasks like opening connections, loading config, etc.
		 */
		@Override
		protected void setup(Context context) {
			logger.debug("Inside Setup");
		}
		
		// Constant value of 1 for counting word occurrences
		private static final IntWritable ONE = new IntWritable(1);
		
		// Reusable Text object for output keys (avoids object creation overhead)
		private Text outputKey = new Text();
		
		/**
		 * Map function that processes each line of input
		 * Called once for each input record (line of text)
		 * Splits line into words and emits each word with count of 1
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			logger.debug("Inside map");
			
			// Convert Text to String for processing
			String currentLine = value.toString();
			
			// Split line by spaces into individual words
			String [] words = StringUtils.split(currentLine, ' ');
			
			// Emit each word with a count of 1
			for(String word : words) {
				outputKey.set(word);
				context.write(outputKey, ONE);
			}
		}
		
		/**
		 * Cleanup method called ONCE after all map() calls complete
		 * Used for cleanup tasks like closing connections, releasing resources, etc.
		 * 
		 * Mapper lifecycle: setup() → map() [called many times] → cleanup()
		 */
		@Override
		protected void cleanup(Context context) {
			logger.debug("Inside CleanUp");
		}
	}
	
	/**
	 * Reducer class that aggregates word counts
	 * Receives all values for a particular word (grouped by MapReduce framework)
	 * Input: Key = word, Values = list of counts (IntWritable)
	 * Output: Key = word, Value = total count
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Reusable IntWritable for output values
		private IntWritable outputValue = new IntWritable();
		
		/**
		 * Reduce function that sums all counts for a given word
		 * Called once for each unique word (key)
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			// Sum all counts for this word
			for(IntWritable count : values) {
				sum += count.get();
			}
			
			// Emit the word with its total count
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Configures and runs the MapReduce job with custom partitioner
	 * @param args Command line arguments:
	 *             args[0] = input path
	 *             args[1] = output path
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create new MapReduce job
		Job job = Job.getInstance(getConf(), "WordCountJob");
		Configuration conf = job.getConfiguration();
		
		// Set the jar class
		job.setJarByClass(getClass());
		
		// Set up input and output paths
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		// Delete output directory if it exists
		out.getFileSystem(conf).delete(out, true);
		
		// Configure input and output paths
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Set Mapper and Reducer classes
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Set custom partitioner to control data distribution
		job.setPartitionerClass(WordCountPartitioner.class);
		
		// Commented out: Set number of reducer tasks
		// Uncomment to use multiple reducers (creates multiple output files)
		// Each reducer will receive keys based on the partitioner logic
//		job.setNumReduceTasks(3);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set intermediate output types (mapper output)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set final output types (reducer output)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Submit job and wait for completion
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * Main entry point for the application
	 * @param args Command line arguments: [input_path] [output_path]
	 * 
	 * Example usage:
	 * hadoop jar wordcount.jar input.txt output
	 * 
	 * With multiple reducers (uncomment setNumReduceTasks in run method):
	 * - Creates multiple output files (part-r-00000, part-r-00001, part-r-00002)
	 * - Each file contains words assigned to that partition by the custom partitioner
	 * - Distribution based on (word_length * count) % numReduceTasks
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			result = ToolRunner.run(new Configuration(), 
							new WordCountJob(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with job result code
		System.exit(result);
	}
}
