package pagerankMR.controller;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import pagerankMR.pagerank.comparators.ReverseComparator;
import pagerankMR.pagerank.job1.PageRankJob1Mapper;
import pagerankMR.pagerank.job1.PageRankJob1Reducer;
import pagerankMR.pagerank.job2.PageRankJob2Mapper;
import pagerankMR.pagerank.job2.PageRankJob2Reducer;
import pagerankMR.pagerank.job3.PageRankJob3Mapper;





public class PageRankController {
	
	public static Set<String> NODES = new HashSet<String>();
	
	// configuration values
	public final static Double DAMPING = 0.85;
	public static int ITERATIONS = 5;
	public static String INPUT_FOLDER = "/home/senko/pagerankInput";
	public static String OUT_PATH = "/home/senko/pagerank/iter";
	public static String RESULT_PATH = "/home/senko/pagerank/result";
	
	public static NumberFormat NF = new DecimalFormat("00");
	
	public static void main(String[] args) throws InterruptedException {
		
		// clean everything for beginning
		File main = new File("/home/senko/pagerank");
		if(main.exists()) {
			try {
				System.out.println("/home/senko/pagerank exists");
				FileUtils.forceDelete(main);
				FileUtils.forceMkdir(main);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		else {
			try {
				FileUtils.forceMkdir(main);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
			
		Thread.sleep(1000);
		
		String inPath = null;;
        String lastOutPath = null;
        PageRankController pagerank = new PageRankController();
        
        System.out.println("Running Job#1 (graph parsing) ...");
        
		try {
			boolean isCompleted = pagerank.job1(INPUT_FOLDER, OUT_PATH + "00");
			if(!isCompleted) {
				System.exit(1);
			}
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(int runs = 0; runs < ITERATIONS; runs++) {
			inPath = OUT_PATH + NF.format(runs);
			lastOutPath = OUT_PATH + NF.format(runs + 1);
			System.out.println("Running Job#2 [" + (runs + 1) + "/" + PageRankController.ITERATIONS + "] (PageRank calculation) ...");
			
			try {
				System.out.println("job2 working for run:" + runs);
				System.out.println("inputFolder: " + inPath + " outputFolder: " + lastOutPath);
				boolean isCompleted = pagerank.job2(inPath, lastOutPath);
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("Running Job#3 (rank ordering) ...");
		try {
			boolean isCompleted = pagerank.job3(lastOutPath, RESULT_PATH);
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}

	private boolean job3(String inputFolder, String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		
		
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        
        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(PageRankController.class);
        
        // Mapper Config
        FileInputFormat.addInputPath(job3, new Path(inputFolder));
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setMapperClass(PageRankJob3Mapper.class);
        
        // output
        FileOutputFormat.setOutputPath(job3, new Path(outputFolder));
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.setSortComparatorClass(ReverseComparator.class);
        
        return job3.waitForCompletion(true);
		
        
        
	}

	private boolean job2(String inputFolder, String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        
        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(PageRankController.class);
        
        // Mapper Config
        FileInputFormat.addInputPath(job2, new Path(inputFolder));
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(PageRankJob2Mapper.class);
        
        // Reducer config
        FileOutputFormat.setOutputPath(job2, new Path(outputFolder));
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setReducerClass(PageRankJob2Reducer.class);
        
        return job2.waitForCompletion(true);
		
	}

	private boolean job1(String inputFolder, String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(PageRankController.class);
        
        // Mapper config
        FileInputFormat.addInputPath(job1, new Path(inputFolder));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setMapperClass(PageRankJob1Mapper.class);
        
        // Reducer config
        FileOutputFormat.setOutputPath(job1, new Path(outputFolder));
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setReducerClass(PageRankJob1Reducer.class);
        
		return job1.waitForCompletion(true);
	}

}
