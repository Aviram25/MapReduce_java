package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce job to calculate the average of a numeric field grouped by a key
 * Processes CSV data from counties file and computes averages
 */
public class AverageJob extends Configured implements Tool {

	// Counters to track the number of valid and invalid records processed
	public enum Counters {BAD_RECORDS, GOOD_RECORDS}

	/**
	 * Mapper class that extracts key-value pairs from input records
	 * Input: Raw text lines from the input file
	 * Output: Key = field at index 1, Value = "field_at_index_9,1"
	 */
	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
		// Reusable objects to avoid creating new objects for each record
		public Text outputKey = new Text();
		public Text outputValue = new Text();
		public final String ONE = ",1"; // Append ",1" to track count for averaging
		
		/**
		 * Validates if a string can be parsed as an integer
		 * @param str The string to validate
		 * @throws NumberFormatException if string is not a valid integer
		 */
		public void checkInt(String str) throws NumberFormatException{
			Integer.parseInt(str);
		}
		
		/**
		 * Map function that processes each line of input
		 * Splits CSV line, validates data, and emits key-value pairs
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the CSV line by comma delimiter
			String [] words = StringUtils.split(value.toString(),'\\', ',');
			
			// Check if the record has exactly 12 fields (valid record structure)
			if (words.length == 12) {
				try {
					// Validate that field at index 9 is a valid integer
					checkInt(words[9]);
					
					// Set the key as field at index 1 (e.g., county name)
					outputKey.set(words[1].trim());
					
					// Set the value as "number,1" (number to sum and count of 1)
					outputValue.set(words[9] + ONE);
					
					// Emit the key-value pair
					context.write(outputKey, outputValue);
					
					// Increment the counter for valid records
					context.getCounter(Counters.GOOD_RECORDS).increment(1);
				}
				catch (NumberFormatException e) {
					// If field 9 is not a valid number, count as bad record
					context.getCounter(Counters.BAD_RECORDS).increment(1);
				}
			}
			else {
				// If record doesn't have 12 fields, count as bad record
				context.getCounter(Counters.BAD_RECORDS).increment(1);
			}
		}

		/**
		 * Cleanup method called after all map tasks complete
		 * Prints counter values for debugging
		 */
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("GOOD RECORDS counter = " + context.getCounter(Counters.GOOD_RECORDS).getValue());
			System.out.println("BAD RECORDS counter = " + context.getCounter(Counters.BAD_RECORDS).getValue());
		}
	}

	/**
	 * Combiner class that performs local aggregation on mapper output
	 * Reduces network traffic by combining values before sending to reducer
	 * Input: Key = group key, Values = list of "sum,count" strings
	 * Output: Key = group key, Value = "total_sum,total_count"
	 */
	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();
		private String COMMA = ",";
		
		/**
		 * Combine function that aggregates partial sums and counts
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;    // Accumulator for sum of all values
			int count = 0;   // Accumulator for total count
			
			// Iterate through all values for this key
			while(values.iterator().hasNext()) {
				String current = values.iterator().next().toString();
				
				// Split the "number,count" string
				String [] words = StringUtils.split(current,'\\', ',');
				
				// Add the number to sum and increment count
				sum += Long.parseLong(words[0]);
				count += Integer.parseInt(words[1]);
			}
			
			// Emit the combined sum and count for this key
			outputValue.set(sum + COMMA + count);
			context.write(key, outputValue);
		}		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// Cleanup method for combiner (currently unused)
		}
	}

	/**
	 * Reducer class that computes the final average for each key
	 * Input: Key = group key, Values = list of "sum,count" strings from combiners
	 * Output: Key = group key, Value = average (double)
	 */
	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		DoubleWritable outputValue = new DoubleWritable();
		
		/**
		 * Reduce function that calculates the final average
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;    // Total sum across all values
			int count = 0;   // Total count across all values
			
			// Aggregate all partial sums and counts from combiners
			while(values.iterator().hasNext()) {
				String current = values.iterator().next().toString();
				
				// Split the "sum,count" string
				String [] words = StringUtils.split(current,'\\',',');
				
				// Accumulate sum and count
				sum += Long.parseLong(words[0]);
				count += Integer.parseInt(words[1]);
			}
			
			// Calculate and emit the average (sum / count)
			outputValue.set(((double) sum)/count);
			context.write(key, outputValue);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// Cleanup method for reducer (currently unused)
		}
	}

	/**
	 * Main run method that configures and executes the MapReduce job
	 * @param arg0 Command line arguments
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// Get configuration
		Configuration conf = super.getConf();
		
		// Create a new MapReduce job
		Job job = Job.getInstance(conf, "AverageJob");
		job.setJarByClass(AverageJob.class);

		// Set up output path and delete if it already exists
		Path out = new Path("average");
		out.getFileSystem(conf).delete(out, true);
		
		// Set input and output paths
		FileInputFormat.setInputPaths(job, "counties");
		FileOutputFormat.setOutputPath(job, out);

		// Configure mapper, reducer, and combiner classes
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageCombiner.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set output key and value types (applies to mapper output)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Wait for job completion and return status
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * Main entry point for the application
	 * @param args Command line arguments
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			result = ToolRunner.run(new Configuration(),  new AverageJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with the job's return code
		System.exit(result);
	}
}
