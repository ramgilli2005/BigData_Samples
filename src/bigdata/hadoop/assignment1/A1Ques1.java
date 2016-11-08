package bigdata.hadoop.assignment1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class A1Ques1 {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);

			Configuration conf = context.getConfiguration();

		}
		
		private Text userId = new Text();
		private Text age = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			if(mydata[0] != null && mydata[1] != null && mydata[0] != null){
				String uId = mydata[0].trim();
				String gender = mydata[1].trim();
				String ag = mydata[2].trim();
				//Check the second token and third token
				if(gender.equalsIgnoreCase("M") && ag.equalsIgnoreCase("7")){
					userId.set(uId);
					age.set(ag);
					context.write(age, userId);
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
			
			//Write the result to reducer directly
			for(Text val: values){
				context.write(key, val);
			}

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
		Job job = new Job(conf, "maleusers");
		job.setJarByClass(A1Ques1.class);
		
		//Let the job know the mapper and reducer class
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Set output key type
		job.setOutputKeyClass(Text.class);
		//Set output value type
		job.setOutputValueClass(Text.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
