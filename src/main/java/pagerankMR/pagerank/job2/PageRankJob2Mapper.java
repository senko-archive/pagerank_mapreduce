package pagerankMR.pagerank.job2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		/*	expected input format is
		 * 	key: indexOfFile values: <NodeA> \t <PageRank> \t <List of links that NodeA referenced to>
		 * 
		 * 	output will emits 2 different records
		 * 	first is
		 * 	key:<NodeA>	 values: |<link1>,<link2>,<link3>... "|" is for distinguish from other type
		 * 
		 * 	second output is
		 * 	key: <link>  value: <pageRank of NodeA> \t <total links of NodeA>
		 * 
		 */
		
		String valueTrimmed = value.toString().trim();
		String[] entrySet = valueTrimmed.split("\t");
		List<String> entryList = Arrays.asList(entrySet);
		String node = entryList.get(0);
		String pageRank = entryList.get(1);
		String links = "";
		if(entryList.size() == 2) {
			System.out.println("this links are empty means that this link gives no link. No need to emit this in type1");
			links = "";
		}
		else {
			links = entryList.get(2);
			
			//System.out.println("value is: " + value.toString());
			//System.out.println("key is: " + key.get() +   "links is:" + links);
			
			
			System.out.println("job2 mapper - node:"+node + " pageRank:"+pageRank + " links:"+links);
			// prepare first type of context
			try {
				System.out.println("job2 mapper - type1 emit key: " + node + " values: " + "|" + links);
				context.write(new Text(node), new Text("|"+links));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// prepare second context
			List<String> otherPages = Arrays.asList(links.split(","));
			otherPages.forEach(link -> {
				Text pageRankWithTotalLink = new Text(pageRank + "\t" + otherPages.size());
				try {
					System.out.println("job2 mapper - type2 emit key: " + link + " value: " + pageRankWithTotalLink.toString());
					context.write(new Text(link), pageRankWithTotalLink);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}

		
		
	}

}
