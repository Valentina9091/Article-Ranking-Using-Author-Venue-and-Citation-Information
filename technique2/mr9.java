package technique2;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class mr9 extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(mr9.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr9(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		Job job = Job.getInstance(config, "mr9");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce1.class);

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
				String VpSplit[]=record2[1].split(Constant.SEPARATOR);
				
				String VpScore=VpSplit[0];
				String outlinkList=VpSplit[2];
				valueBuffer.append(VpScore);
				valueBuffer.append(Constant.SEPARATOR);
				valueBuffer.append("PV");
				
				
				if(outlinkList.contains(Constant.CITATION_IDENTIFIER))
				{
					String[] currentOutlink=outlinkList.split(Constant.CITATION_IDENTIFIER);
					for(int i=0;i<currentOutlink.length;i++)
					{
						context.write(new IntWritable(Integer.parseInt(currentOutlink[i])),new Text(valueBuffer.toString()));
					}
				}
				else
				{
					context.write(new IntWritable(Integer.parseInt(outlinkList)),new Text(valueBuffer.toString()));

				}
				
			}
		}}
		
		public static class Reduce1 extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
		StringBuffer valueBuffer=new StringBuffer();

		for(Text value:valueList){
			if(!(valueBuffer.toString().contains(value.toString())))
			{
			valueBuffer.append(value);
		}
		}
		context.write(key, new Text(valueBuffer.toString()));
		
	}
		}
		
	}





