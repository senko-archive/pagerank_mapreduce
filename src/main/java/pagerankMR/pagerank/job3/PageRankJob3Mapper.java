package pagerankMR.pagerank.job3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankJob3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		/* Rank Ordering (mapper only)
	     * Input file format (separator is TAB):
	     * 
	     *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
	     * 
	     * This is a simple job which does the ordering of our documents according to the computed pagerank.
	     * We will map the pagerank (key) to its value (page) and Hadoop will do the sorting on keys for us.
	     * There is no need to implement a reducer: the mapping and sorting is enough for our purpose.
	     */
		
		List<String> entrySet = Arrays.asList(value.toString().split("\t"));
		String title = entrySet.get(0);
		Double pagerank = Double.valueOf(entrySet.get(1));
		
		try {
			context.write(new DoubleWritable(pagerank), new Text(title));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	

}
