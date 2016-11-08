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

public class A1Ques3 {
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		
		private String usrip;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);

			Configuration conf = context.getConfiguration();
			usrip = conf.get("genretype");

		}
		
		private Text movie = new Text();
		private Text genre = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("::");
			if(mydata[1] != null && mydata[2] != null ){
				String movNam = mydata[1];
				String gen = mydata[2];
				if(gen.contains(usrip)){
					movie.set(movNam);
					genre.set(gen);
					context.write(movie, genre);
				}
			}
		}
		
	}
	
	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
		
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
		
		if(otherArgs.length != 3){
			System.err.println("Usage: <ip> <out> <genre_type>");
			System.exit(2);
		}
		
		//Init cap the user input and set it in config
		String ip = otherArgs[2].toLowerCase();
		ip = ip.substring(0,1).toUpperCase().concat(ip.substring(1, ip.length()));
		System.out.println("Genre: "+ip);
		conf.set("genretype", ip);
		
		//Creating a job
		Job job = new Job(conf, "movietype");
		job.setJarByClass(A1Ques3.class);
		
		//Let the job know the mapper and reducer class
		job.setMapperClass(Map3.class);
		job.setReducerClass(Reduce3.class);
		
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
