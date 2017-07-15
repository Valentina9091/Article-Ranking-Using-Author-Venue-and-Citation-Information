package technique2;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class mr7 extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(mr7.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr7(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = Job.getInstance(config, "mr7");
		job.setJarByClass(this.getClass());
		Path output = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			
			for(String record:recArr){
				String[] record2=record.split("\t");
				context.write(new Text(record2[0]),new Text(record2[1]));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregatePaperscore=0.0;
			double Ar=0.0;

			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer myBuffer=new StringBuffer();
			
			for(Text value:valueList){
				if(value.toString().contains(Constant.SEPARATOR))
				{
					String ApscoreSplit[]=value.toString().split(Constant.SEPARATOR);
					aggregatePaperscore+=Double.parseDouble(ApscoreSplit[0]);	

					valueBuffer.append((ApscoreSplit[1]));
					
				
				}
				else
				{
					aggregatePaperscore+=Double.parseDouble(value.toString());	
				}
				}
			Ar=aggregatePaperscore/(float)2;
			myBuffer.append(Ar);
			myBuffer.append(Constant.SEPARATOR);
			if(!(valueBuffer.toString().isEmpty())){
			myBuffer.append(valueBuffer.toString());}
			else
			{
				myBuffer.append(" ");
			}
			myBuffer.append(Constant.SEPARATOR);

			context.write(key, new Text((myBuffer.toString())));
		}
	}

}


