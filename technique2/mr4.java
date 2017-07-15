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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class mr4 extends Configured implements Tool {
	
	static final Logger LOG = Logger.getLogger(mr4.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr4(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		
		Job job = Job.getInstance(config, "mr4");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]) );

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
		
			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			for(String record:recArr){
				
				String[] value=record.split("\t");
			String authorSplit[]=value[1].split(Constant.SEPARATOR);
			String authors=authorSplit[1];
			String paperId=value[0];
			String paperScore=authorSplit[0];
			valueBuffer.append(paperId);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(paperScore);
			
			
		if(authors.contains(Constant.AUTHORSEPARATOR))
			{
				String currentAuthor[]=authors.split(Constant.AUTHORSEPARATOR);
			for(int i=0;i<currentAuthor.length;i++)	
			{
				if(!(currentAuthor[i].trim().isEmpty()))
						{	context.write(new Text(currentAuthor[i].trim()), new Text(valueBuffer.toString()) );
				//LOG.info(currentAuthor[i].trim() +"HEREEEEEEEE");}
			}
			}
			}
			else if(!(authors.trim().isEmpty()))
			{
				context.write(new Text(authors.trim()), new Text(valueBuffer.toString()) );
				//LOG.info(authors.trim() +"HEREEEEE222222222222222");
			}
			else
			{
				continue;
			}
			
			
			
		}}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregatePaperscore=0.0;
			double Ap=0.0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer paperidBuffer=new StringBuffer();
			
			for(Text value:valueList){
				
					String[] paperscoreSplit=value.toString().split(Constant.SEPARATOR);
					String currentPaperscore=paperscoreSplit[1];
					String paperId=paperscoreSplit[0];
					paperidBuffer.append(paperId);
					if(!( paperidBuffer.toString().isEmpty() ))
					{paperidBuffer.append("#%");}	
				
					
					aggregatePaperscore+=Double.parseDouble(currentPaperscore.toString());				
					counter++;
			}
			Ap=aggregatePaperscore/(float)counter;
			valueBuffer.append(Ap);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(paperidBuffer.toString());
			valueBuffer.append(Constant.SEPARATOR);
			context.write(key, new Text(valueBuffer.toString()));
		}
	}

}


