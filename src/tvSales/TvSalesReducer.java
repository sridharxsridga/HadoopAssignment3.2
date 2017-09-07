/*
 * TvSalesReducer is a reducer class whose input key is 1 and input value is record (entire line)
 * and null as key and records eliminating bad records as  value
 */
package tvSales;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TvSalesReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	/*
	 * reduce method is called for each <key, (collection of values)> in the sorted inputs.
	 * Here null is given as key and records as value which will be list of values stored from intermediate result of map function
	 * 
	 */

	public void reduce(IntWritable Key, Iterable<Text> outputRecords, Context context)
			throws IOException, InterruptedException {
		
		/*
		 * Iterate outputRecords value  which we get after shuffle and sort phase
		 * and write records
		 * The output of the reduce task is typically written to a RecordWriter
	     * Context is used for write operation in map reduce program, here null will be send as key and records will be sent as value
	     */
		
		for(Text records : outputRecords){
			context.write(NullWritable.get(), records);
		}
		
	}
}