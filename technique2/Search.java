package technique2;

import java.io.IOException;

import java.util.logging.Logger;

//import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Search extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(Search.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new Search(), args);
	}

	public int run(String[] args) throws Exception {

		
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter", "#index");
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(getConf(), "LinkGraph");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)? 0:1;
		
/*		Configuration config = getConf();

		Job job2 = Job.getInstance(config, "mr7");
		job2.setJarByClass(this.getClass());
		
		MultipleInputs.addInputPath(job2,new Path(args[1]+"out"),TextInputFormat.class,Map2.class);
		MultipleInputs.addInputPath(job2,new Path(args[2]),TextInputFormat.class,Map2.class);
		TextOutputFormat.setOutputPath(job2, new Path(args[3]));

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;*/
	}


	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			String line=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(line);
			String title;
			
			StringBuffer ValueBuffer=new StringBuffer();
			StringBuffer myBuffer=new StringBuffer();
			String value="";
			 value=recArr[0].trim();
			 String outlink="";

			for (String record : recArr) {
				if(!(record.isEmpty())){
				if (record.startsWith(Constant.TITLE_IDENTIFIER)) {
					
					title=(record.toString().replace(Constant.TITLE_IDENTIFIER,"").trim());
					if(title.trim().isEmpty())
					{
						ValueBuffer.append(" ");
						
					}
					else
					{
					ValueBuffer.append(title);
					}
					context.write(new IntWritable(Integer.parseInt(value)), new Text(ValueBuffer.toString()));
				}
				else if(record.startsWith(Constant.CITATION_IDENTIFIER))
				{
					outlink=(record.toString().replace(Constant.CITATION_IDENTIFIER,"").trim());
					if(outlink.trim().isEmpty())
					{
						myBuffer.append(" ");
						
					}
					else
					{
					myBuffer.append(outlink);
					context.write(new IntWritable(Integer.parseInt(outlink)), new Text(value+Constant.SEPARATOR));
					}
					
				}
				

			}}
			
		}
	}
	public static class Reduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			StringBuffer myBuffer=new StringBuffer();			
			StringBuffer titleBuffer=new StringBuffer();
			StringBuffer dataBuffer=new StringBuffer();
			int counter=0;
			
			for(Text value:valueList){
			if(value.toString().contains(Constant.SEPARATOR))
			{
				if(counter>0)
				{
					myBuffer.append(Constant.CITATION_IDENTIFIER);
				}
				String outlink_value=value.toString().replace(Constant.SEPARATOR,"");
				myBuffer.append(outlink_value);
				  counter++;
			}
			else
			{
				titleBuffer.append(value.toString());
			}
			}
			
			dataBuffer.append(titleBuffer.toString());
			dataBuffer.append(Constant.SEPARATOR);
			
			if(myBuffer.toString().isEmpty())
			{	
				dataBuffer.append(" ");
				dataBuffer.append(Constant.SEPARATOR);
			}
			else
			{
				dataBuffer.append(myBuffer.toString());
				dataBuffer.append(Constant.SEPARATOR);
			}
			context.write(key, new Text((dataBuffer.toString())));
		}
	

}
}


