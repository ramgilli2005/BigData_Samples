package bigdata.hadoop.assignment1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class A1Ques2 {
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();

		}
		
		private final static IntWritable one = new IntWritable(1);  
		private Text genAge = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			if(mydata[1] != null && mydata[0] != null){
				String gen = mydata[1].trim();
				String ag = mydata[2].trim();
				genAge.set(ag+" "+gen);
				context.write(genAge, one);
			}
		}
		
	}
	
	public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException{
			
			int sum = 0;
			//Write the result to reducer directly
			for(IntWritable val: values){
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);

		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.err.println("Usage: <ip> <out>");
			System.exit(2);
		}
		
		//Creating a job
		Job job = new Job(conf, "useragegrp");
		job.setJarByClass(A1Ques2.class);
		
		//Let the job know the mapper and reducer class
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		
		//Set output key type
		job.setOutputKeyClass(Text.class);
		//Set output value type
		job.setOutputValueClass(IntWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
