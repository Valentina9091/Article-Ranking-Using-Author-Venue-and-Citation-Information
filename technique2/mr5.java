
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
	public class mr5 extends Configured implements Tool {
		
		static final Logger LOG = Logger.getLogger(mr5.class.getName());
		public static void main(String[] args) throws Exception {
			Configuration conf= new Configuration(true);
			@SuppressWarnings("unused")
			int res = ToolRunner.run(conf,new mr5(), args);
		}

		public int run(String[] args) throws Exception {
			Configuration config = getConf();

			Path output = new Path(args[1]);
			FileSystem hdfs = FileSystem.get(config);

			// delete existing directory
			if (hdfs.exists(output)) {
				hdfs.delete(output, true);
			}
			Job job = Job.getInstance(config, "mr5");
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
				StringBuffer mybuffer=new StringBuffer();
				
				String data=lineText.toString();
				String[] recArr=Constant.LINEPATTERN.split(data);
				for(String record:recArr){
					String[] currentvalue=record.split("\t");
					String[] valueSplit=currentvalue[1].split(Constant.SEPARATOR);
					String pageScore=valueSplit[0];
					String venue=valueSplit[2];
					//LOG.info(venue +"VENUEEEEEEEEEEEEE111111111111111111111");
					valueBuffer.append(pageScore);
					
					String paperId=currentvalue[0];
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(paperId);
					valueBuffer.append(Constant.SEPARATOR);
					
						//LOG.info(venue +"VENUEEEEEEEEEEEEE2222222222222222222222");
						
					if(valueSplit[1].contains(Constant.AUTHORSEPARATOR)){
						if(!(venue.trim().isEmpty())){
						String authors[]=valueSplit[1].split(Constant.AUTHORSEPARATOR);
					for(int i=0;i<authors.length;i++)
					{
					mybuffer.append(valueBuffer);
					if(!(authors[i].trim().isEmpty())){
					mybuffer.append(authors[i]);
					mybuffer.append(Constant.SEPARATOR);
					context.write(new Text(venue),new Text(mybuffer.toString()));
					mybuffer.setLength(0);
					}					
					}
					
					}
						

						}
					else
					{
						if(!(venue.trim().isEmpty())){
						mybuffer.append(valueBuffer);
						if(!(valueSplit[1].trim().isEmpty())){
						mybuffer.append(valueSplit[1]);
						mybuffer.append(Constant.SEPARATOR);
						context.write(new Text(venue),new Text(mybuffer.toString()));
						mybuffer.setLength(0);
						}
						else
						{
							mybuffer.append(valueBuffer);
							mybuffer.append(" ");
							mybuffer.append(Constant.SEPARATOR);
							context.write(new Text(venue),new Text(mybuffer.toString()));
							mybuffer.setLength(0);

						}
						
					}
					
					
					}
					
					
					
				}
				
			}
		}

		public static class Reduce extends
				Reducer<Text, Text, Text, Text> {
			@Override
			public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
				double aggregatePaperscore=0.0;
				double Vp=0.0;
				int counter=0;
				StringBuffer valueBuffer=new StringBuffer();
				StringBuffer paperidBuffer=new StringBuffer();
				StringBuffer authorBuffer=new StringBuffer();

				
				for(Text value:valueList){
					
						String[] paperscoreSplit=value.toString().split(Constant.SEPARATOR);
						String currentPaperscore=paperscoreSplit[0];
						String paperId=paperscoreSplit[1];
						
						String author=paperscoreSplit[2];
					
						
						if(!( paperidBuffer.toString().isEmpty() ))
						{paperidBuffer.append("#%");}	
						paperidBuffer.append(paperId);
						
						
						if(!(authorBuffer.toString().contains(author))){
							if(!( authorBuffer.toString().isEmpty() ))
							{authorBuffer.append("@@");}
							
							authorBuffer.append(author);}	
						aggregatePaperscore+=Double.parseDouble(currentPaperscore.toString());				
						counter++;
				}
				Vp=aggregatePaperscore/(float)counter;
				valueBuffer.append(Vp);
				valueBuffer.append(Constant.SEPARATOR);
				valueBuffer.append(authorBuffer.toString());
				valueBuffer.append(Constant.SEPARATOR);
				valueBuffer.append(paperidBuffer.toString());
				valueBuffer.append(Constant.SEPARATOR);
				
				context.write(key, new Text(valueBuffer.toString()));
			}
		}

	}



