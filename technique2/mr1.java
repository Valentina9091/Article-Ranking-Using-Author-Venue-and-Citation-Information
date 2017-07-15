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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class mr1  extends Configured implements Tool {

	static final Logger LOG =Logger.getLogger(mr1.class.getName());


	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new mr1(), args);
	}

	public  int run(String[] args) throws Exception {	

		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter","#index");
		
		Job job = Job.getInstance(conf, "mr1");
		job.setJarByClass(this.getClass());	
		
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		
		Path output2 = new Path(args[2]);

		// delete existing directory
		if (hdfs.exists(output2)) {
			hdfs.delete(output2, true);
		}
		
		// The input and output be the sequence file i/o
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		int maxIndegree=(int) job.getCounters().findCounter("maxIndegree","maxIndegree").getValue();
		int maxOutdegree=(int) job.getCounters().findCounter("maxOutdegree","maxOutdegree").getValue();

		conf.setInt("maxOutdegree", maxOutdegree);
		conf.setInt("maxIndegree", maxIndegree);
		
		Job job2 = Job.getInstance(conf, "mr2");
		job2.setJarByClass(this.getClass());	

		// The input and output be the sequence file i/o
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.setMapperClass(Map2.class);
		//job2.setReducerClass(Reduce2.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		return job2.waitForCompletion(true)? 0:1;
		
	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		

		
		@Override 
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			int maxOutdegree=(int)context.getCounter("maxOutdegree","maxOutdegree").getValue();
			int OutDegree=0;
			
			
			
			String line=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(line);
			String AuthorList;
			String[] Authors;
			String Venue;
			StringBuffer ValueBuffer=new StringBuffer();
			StringBuffer OutlinkBuffer=new StringBuffer();

			if(!(recArr[0].isEmpty()))
			{
			String value=recArr[0].trim();
			
				for(String record:recArr){
					
						if(record.startsWith(Constant.AUTHOR_IDENTIFIER))
						{
							ValueBuffer.append(Constant.SEPARATOR);
							AuthorList=(record.toString().replace(Constant.AUTHOR_IDENTIFIER,"").trim());
							if(AuthorList.isEmpty())
							{
								ValueBuffer.append(" ");
							}
							else if(AuthorList.trim().contains(";"))
							{
							Authors=AuthorList.trim().split(";");
							for(int i=0;i<Authors.length;i++)
							{
								ValueBuffer.append(Authors[i]);
								if(i!=Authors.length-1){
								ValueBuffer.append(Constant.AUTHORSEPARATOR);}

							}	
						}
							else
							{
								ValueBuffer.append(AuthorList);
							}
							ValueBuffer.append(Constant.SEPARATOR);

						}
						
						if(record.startsWith(Constant.VENUE_IDENTIFIER))
						{
							
							
							Venue=(record.toString().replace(Constant.VENUE_IDENTIFIER,"").trim());
							if(Venue.isEmpty())
							{
								ValueBuffer.append(" ");
								
							}
							else
							{
							ValueBuffer.append(Venue);
							}
							ValueBuffer.append(Constant.SEPARATOR);
						}
				
						if(record.startsWith(Constant.CITATION_IDENTIFIER))
						{							
						
							if(OutDegree>0)
							{
								OutlinkBuffer.append("#%");

							}
							int OutLink = Integer.parseInt(record.toString().replace(Constant.CITATION_IDENTIFIER,"").trim());
							OutlinkBuffer.append(OutLink);
							
							context.write(new IntWritable((OutLink)), new Text("1"));
							OutDegree++;
							
						}
						
						
				
				}
				if((OutlinkBuffer.toString().isEmpty()))
				{	
					
					OutlinkBuffer.append(" ");
					
				}
				
				if(maxOutdegree<OutDegree)
				{
					context.getCounter("maxOutdegree","maxOutdegree").setValue(OutDegree);

					
				}
				ValueBuffer.append(OutlinkBuffer);
				ValueBuffer.append(Constant.SEPARATOR);
				context.write(new IntWritable(Integer.parseInt(value)), new Text(ValueBuffer.toString()));
				}	
		}
		
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text > {	
		@Override 
		public void reduce(IntWritable keyText, Iterable<Text> valueList, Context context) throws IOException, InterruptedException {
			StringBuffer ValueBuffer=new StringBuffer();
			int maxIndegree=(int)context.getCounter("maxIndegree","maxIndegree").getValue();
			int inDegree=0;
			
			for(Text value:valueList){
				if(value.toString().contains(Constant.SEPARATOR))
				{
					ValueBuffer.append(value);
				}
				else
				{
					inDegree+=Integer.parseInt(value.toString());
				}
				
			}
			if(inDegree>maxIndegree)
			{
				context.getCounter("maxIndegree","maxIndegree").setValue(inDegree);
			}
			ValueBuffer.append(Constant.INDEGREE);
			ValueBuffer.append(inDegree);

			context.write(keyText,new Text(ValueBuffer.toString()));
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		@Override 
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			double alpha= 1.7;
			double beta=3;
			double MaxIndegree=Double.parseDouble(context.getConfiguration().get("maxIndegree"));
			double MaxOutdegree=Double.parseDouble(context.getConfiguration().get("maxOutdegree"));
			String[] recArr=Constant.LINEPATTERN.split(data);
			for(String record:recArr){
				
				String[] currentPaper=record.split("\t");
				String paperId=currentPaper[0];
				String paperInfo=currentPaper[1];
				
				String[] indegreeSplit=paperInfo.split("INDEGREE");
				int paperIndegree=Integer.parseInt(indegreeSplit[1]);
				
				String[] outdegreeSplit=paperInfo.split("#%");
				int paperOutdegree=((outdegreeSplit.length)-1);
				
				double Numerator= (alpha * (paperIndegree/MaxIndegree));
				Numerator+= Math.pow((double)paperOutdegree/MaxOutdegree, (double)beta);
				double denominator=1+alpha;
				double paper_score=Numerator/denominator;
				
				valueBuffer.append(paper_score);
				valueBuffer.append(indegreeSplit[0]);
				
				context.write(new IntWritable(Integer.parseInt(paperId)), new Text(valueBuffer.toString()));
				valueBuffer.setLength(0);
			}
			
		
	}
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text > {	
		@Override 
		public void reduce(IntWritable keyText, Iterable<Text> valueList, Context context) throws IOException, InterruptedException {
			StringBuffer ValueBuffer=new StringBuffer();
			Configuration conf = context.getConfiguration();
			int MaxIndegree=Integer.parseInt(conf.get("MaxIndegree"));
			int InDegree=0;
			
			for(Text value:valueList){
				if(value.toString().contains(Constant.SEPARATOR))
				{
					ValueBuffer.append(value);
				}
				else
				{
					InDegree+=Integer.parseInt(value.toString());
				}
				
			}
			if(InDegree>MaxIndegree)
			{
				conf.setInt("MaxIndegree", InDegree);
			}
			ValueBuffer.append(Constant.INDEGREE);
			ValueBuffer.append(InDegree);

			context.write(keyText,new Text(ValueBuffer.toString()));
		}
	}

}


