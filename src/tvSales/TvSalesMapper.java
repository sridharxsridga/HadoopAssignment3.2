/*
 * TvSalesMapper is a mapper class which takes input as TextInputFormat whose input key is ByteOffset and input value is entire line
 * and writes 1 as key and entire line as value by eliminating bad records
 */

package tvSales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Extending Mapper which Maps input key/value pairs to a set of intermediate key/value pairs.
 * 
 */
public class TvSalesMapper extends Mapper<LongWritable,Text,IntWritable,Text>{

	/*
	 * Maps are the individual tasks which transform input records into a intermediate records
	 * It takes input key as byteOffset ,since the default input and output format is TextInputFormat and TextOutputFormat respectively
	 * It takes value as the entire line
	 * This is called for each key/value pair in the InputSplit
	 */
	
    public void map(LongWritable inputKey, Text inputValue,Context context) throws IOException, InterruptedException{
    	//if does not contain bad records then write
        if(isBadRecord(inputValue)==false)
        context.write(new IntWritable(1), inputValue);
    }
    
    /*Method to check if Company name and product name contains bad records "NA",
     * Return true if contains bad records and false if does not contain bad records 
     */
    private boolean isBadRecord(Text record){
        String[] lineArray= record.toString().split("\\|");//Splitting the line using separator |
        System.out.println(lineArray[0]); //Storing comapnyName 
        System.out.println(lineArray[1]);//Storing product name
        if ( (lineArray[0].equals("NA")) || (lineArray[1].equals("NA")) )//check if Company name and product name contains bad records "NA"
            return true;
        else
            return false; //returns false if does not contain bad records
        
    }
}