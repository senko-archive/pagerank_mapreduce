package pagerankMR.pagerank.job1;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pagerankMR.controller.PageRankController;

public class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		/*	Job1 reducer will get <NodeA> and other nodes that NodeA referencing to (if A gives link ta Y and X, A->Y, A->X then it will gather A(X,Y) )
		 * 	and reducer will gather all referenced to web sites and give initial pagerank
		 * 	it emits this format
		 * 	key: <NodeA>, values : InitialPageRank \t Set of Links
		 */
		
		
		Double initialPageRank = (PageRankController.DAMPING / new Double(PageRankController.NODES.size()));
		System.out.println("all node number is: " + PageRankController.NODES.size());
		System.out.println("initial page rank is: " + initialPageRank);
		String initialPGasString = initialPageRank.toString();
		
		Stream<Text> streamOfValues = StreamSupport.stream(values.spliterator(), false);
		String referencedNodes = streamOfValues.map(value -> value.toString()).collect(Collectors.joining(",")).toString();
		System.out.println("referencedNodes: " + referencedNodes);
		
		StringBuilder emitValues = new StringBuilder(initialPGasString).append("\t").append(referencedNodes);
		
		try {
			System.out.println("Job1 Reducer context.write key: " + key.toString() + " values: " + emitValues.toString());
			context.write(key, new Text(emitValues.toString()));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
