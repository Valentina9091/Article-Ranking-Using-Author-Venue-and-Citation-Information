package technique2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;

public class Driver {

//	private static final Logger LOG = Logger.getLogger(Driver.class);

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration(true);

		args[0]="/home/cloudera/Downloads/input.txt";
		args[1]="/home/cloudera/Downloads/Technique1/mr1_out";
		args[2]="/home/cloudera/Downloads/Technique1/mr2_out";
		int res = ToolRunner.run(conf, new mr1(), args);
		if (res == 0) {
			args[0]=args[2];
			args[1]="/home/cloudera/Downloads/Technique1/mr3_out";
			
				res = ToolRunner.run( new mr3(), args);
				if (res == 0) {
					args[0]="/home/cloudera/Downloads/Technique1/mr2_out/";
					args[1]="/home/cloudera/Downloads/Technique1/mr4_out";

					 res = ToolRunner.run(new mr4(), args);
					if (res == 0) {
						args[0]="/home/cloudera/Downloads/Technique1/mr2_out/";
						args[1]="/home/cloudera/Downloads/Technique1/mr5_out";
						
					res = ToolRunner.run(new mr5(), args);
					if (res == 0) {
							args[0]=args[1];
							args[1]="/home/cloudera/Downloads/Technique1/mr6_out";
							
							res = ToolRunner.run( new mr6(), args);
							if (res == 0) {
								args[0]=args[1]+"/*";
								args[1]="/home/cloudera/Downloads/Technique1/mr4_out/*";
								args[2]="/home/cloudera/Downloads/Technique1/mr7_out";
								
								res = ToolRunner.run(new mr7(), args);	
								if (res == 0) {
									args[0]=args[2]+"/*";
									args[1]="/home/cloudera/Downloads/Technique1/mr8_out";
									
									
									res = ToolRunner.run(new mr8(), args);	
									if (res == 0) {
										args[0]="/home/cloudera/Downloads/Technique1/mr5_out/*";
										args[1]="/home/cloudera/Downloads/Technique1/mr9_out";
										
										res = ToolRunner.run(new mr9(), args);	
										if (res == 0) {
											args[0]="/home/cloudera/Downloads/Technique1/mr2_out/*";
											args[1]="/home/cloudera/Downloads/Technique1/mr3_out/*";
											args[2]="/home/cloudera/Downloads/Technique1/mr8_out/*";
											args[3]="/home/cloudera/Downloads/Technique1/mr9_out/*";
											
											args[4]="/home/cloudera/Downloads/Technique1/mr10_out";
											
											res = ToolRunner.run(new mr10(), args);	
							if (res == 0) {
								System.exit(res);
							}
						}
					}
				}

			}
		}
	}
}
		}
	}
}