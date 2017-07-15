package technique2;



import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class mr3 extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(mr3.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr3(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "mr3");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, FloatWritable> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			
			for(String record:recArr){
				
				String[] record2=record.split("\t");
				//LOG.info(record2[1]+"HEREEEEEEEE");
				String outlinkSplit[]=record2[1].split(Constant.SEPARATOR);
				String paperScore=(outlinkSplit[0]);
				String outlinkList=outlinkSplit[3];
				
				
				
				if(!((outlinkList.trim()).isEmpty()))	
				{
					if(outlinkList.contains(Constant.CITATION_IDENTIFIER)){
					String outLinks[]=outlinkList.split(Constant.CITATION_IDENTIFIER);
					for(int i=0;i<outLinks.length;i++)
					{
					context.write(new IntWritable(Integer.parseInt(outLinks[i])), new FloatWritable(Float.parseFloat(paperScore)));	
					}	
				}
				}
					
				else if(!(outlinkList.trim().contains("#%")))	
				{
					if(!((outlinkList.trim()).isEmpty()))	

					{
						
					context.write(new IntWritable(Integer.parseInt(outlinkList.trim())), new FloatWritable(Float.parseFloat(paperScore)));
				}
					}
				
			
			}
		
		
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, FloatWritable, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<FloatWritable> valueList,Context context) throws IOException, InterruptedException {
			double aggregatePaperscore=0.0;
			double pc=0.0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();
			
			for(FloatWritable value:valueList){
					aggregatePaperscore+=Double.parseDouble(value.toString());				
					counter++;
			}
			
			pc=aggregatePaperscore/(float)counter;
			valueBuffer.append(pc);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append("PC");
			
			context.write(key, new Text(valueBuffer.toString()));
			
		}
	}

}


