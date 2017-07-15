package technique2;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import static technique2.Constant.*;
public class mr10 extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(mr10.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr10(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = Job.getInstance(config, "mr10");
		job.setJarByClass(this.getClass());


		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,Map.class);
		TextOutputFormat.setOutputPath(job, new Path(args[4]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			
			for(String record:recArr){
				String[] record2=record.split("\t");

				if(StringUtils.countMatches(record2[1], Constant.SEPARATOR)>1)
				{
					String pagescoreSplit[]=record2[1].split(Constant.SEPARATOR);
					valueBuffer.append(pagescoreSplit[0]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append("PI");
					context.write(new IntWritable(Integer.parseInt(record2[0])), new Text(valueBuffer.toString()));
					valueBuffer.setLength(0);
					valueBuffer.append(pagescoreSplit[1]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(pagescoreSplit[2]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(pagescoreSplit[3]);
					valueBuffer.append(Constant.SEPARATOR);
					context.write(new IntWritable(Integer.parseInt(record2[0])), new Text(valueBuffer.toString()));
				}
				else
				{
				context.write(new IntWritable(Integer.parseInt(record2[0])), new Text((record2[1])));
				}	
					
				
			}

		}
	}

	public static class Reduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			float gama=(float) 0.3;
			float paperScore=0;
			float PI=0;
			float PA=0;
			float PV = 0;
			float PC=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer currentBuffer=new StringBuffer();
			
			for(Text value:valueList){
				String currentValue=value.toString();
				if(StringUtils.countMatches(currentValue, Constant.SEPARATOR)>1)
				{
					valueBuffer.append(currentValue);
				}
				else if(currentValue.contains("###PI"))
				{
					 PI=Float.parseFloat((currentValue.split("###PI"))[0]);
				}
				else if(currentValue.contains("###PA"))
				{
					
					 PA=Float.parseFloat((currentValue.split("###PA"))[0]);
				}
				else if(currentValue.contains("###PV"))
				{
					 PV=Float.parseFloat((currentValue.split("###PV"))[0]);
				}
				else if(currentValue.contains("###PC"))
				{
					 PC=Float.parseFloat((currentValue.split("###PC"))[0]);
				}
				else
				{
					break;
				}
			
				
}			
			float aggregate=PA+PV+PC/3;
			paperScore= ( gama * PI ) + (( 1 - gama ) * aggregate);
			currentBuffer.append(paperScore);
			currentBuffer.append(Constant.SEPARATOR);

			currentBuffer.append(valueBuffer.toString());
			
				context.write(key,new Text(currentBuffer.toString()));
			
		}
}
	}




