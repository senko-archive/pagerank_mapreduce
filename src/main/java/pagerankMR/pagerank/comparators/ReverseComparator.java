package pagerankMR.pagerank.comparators;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseComparator extends WritableComparator {
	
	private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();
	
	public ReverseComparator() {
		super(DoubleWritable.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	       return (-1)* DOUBLE_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
	    }
	
	@SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof DoubleWritable && b instanceof DoubleWritable) {
			return (-1)*(((DoubleWritable) a).compareTo((DoubleWritable) b));
		}
		return super.compare(a, b);
	}

}
