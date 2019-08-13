package pagerankMR.pagerank.job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pagerankMR.controller.PageRankController;

public class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		/*
		 *  Reducer waits two type of input
		 *  one is   key:<NodeA>  values: |<link1>,<link2>,<link3>...
		 *  other is key: <link>  value: <pageRank of NodeA> \t <total links of NodeA>
		 */
		
		String links = "";
        double sumShareOtherPageRanks = 0.0;
        
        for(Text value : values) {
        	String content = value.toString();
        	/*	 if format is below
        	 *	 <NodeA>   |<link1>,<link2>,<link3>,<link4>, ... , <linkN>
        	 */
        	if(content.startsWith("|")) {
        		 // for future use: this is needed to reconstruct the input for Job#2 mapper
                 // in case of multiple iterations of it.
        		links += content.substring("|".length());
        	}
        	else {
        		// if format is : <link> \t <pageRank of NodeA> \t <total links of NodeA>
        		String[] split = content.split("\t");
        		
        		// extract tokens
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                
                // add the contribution of all the pages having an outlink pointing 
                // to the current node: we will add the DAMPING factor later when recomputing
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPageRanks += (pageRank / totalLinks);
        	}
        	
        }
        // apply pageRank formula
        double newRank = PageRankController.DAMPING * sumShareOtherPageRanks + (1 - PageRankController.DAMPING);
        try {
        	System.out.println("job2 reducer - key: " + key + " newrank:" + newRank + " links:" + links);
			context.write(key, new Text(newRank + "\t" + links));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
