package pagerankMR.pagerank.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerankMR.controller.PageRankController;

public class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		/*	job1 takes input as below 
		 *  fromNode	toNode
		 *  <NodeA>		<NodeB>
		 *  
		 *  and emits as key, value pair key:<NodeA>, value:<NodeB>
		 */
		
		String line = value.toString();
		// input data is tabular
		String[] entrySet = line.split("\t");
		
		String NodeA = entrySet[0];
		String NodeB = entrySet[1];
		
		try {
			// emit key as <NodeA>, value as <NodeB>
			context.write(new Text(NodeA), new Text(NodeB));
			
			// add the current source node to the node list so we can 
            // compute the total amount of nodes of our graph in Job#2
			PageRankController.NODES.add(NodeA);
			
			// also add the target node to the same list: we may have a target node 
            // with no outlinks (so it will never be parsed as source)
			PageRankController.NODES.add(NodeB);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
