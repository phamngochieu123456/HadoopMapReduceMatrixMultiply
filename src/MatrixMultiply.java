import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
 
class MatrixMultiplyMapper extends Mapper <LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context outputMap) throws IOException, InterruptedException 
	{
		Configuration conf = outputMap.getConfiguration();
		int m = Integer.parseInt(conf.get("m"));
		System.out.println("m la: " + m);
		int n = Integer.parseInt(conf.get("n"));
		System.out.println("n la: " + n);
		String line = value.toString();
		System.out.println("value la: " + value.toString());
 
		// (M, i, j, Mij);
		String[] indicesAndValue = line.split(",");
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		System.out.println("indices and value la: " + indicesAndValue[0] + ", " + indicesAndValue[1] + ", " + indicesAndValue[2] + ", " + indicesAndValue[3]);
		
		if (indicesAndValue[0].equals("M")) 
		{
			System.out.println("Trong M");
			for (int k = 0; k < n; k++) 
			{
				outputKey.set(indicesAndValue[1] + "," + k);
				// outputKey.set(i,k);
				outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2] + "," + indicesAndValue[3]);
				// outputValue.set(M,j,Mij);
				outputMap.write(outputKey, outputValue);
			}
		} 
		else 
		{
			System.out.println("Trong N");
			// (N, j, k, Njk);
			for (int i = 0; i < m; i++) 
			{
				outputKey.set(i + "," + indicesAndValue[2]);
				outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]);
				outputMap.write(outputKey, outputValue);
			}
		}
	}
}//class MatrixMultiplyMapper


class MatrixMultiplyReduce extends Reducer <Text, Text, Text, Text> 
{
	public void reduce(Text key, Iterable<Text> values, Context outputReduce) throws IOException, InterruptedException 
	{
		System.out.println("Key trong reduce la: " + key.toString());
		String[] value;
		//key=(i,k),
		//Values = [(M/N,j,V/W),..]
		HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
		HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
		System.out.println("Values trong reduce la: ");
		for (Text val : values) 
		{
			System.out.println(val.toString());
			value = val.toString().split(",");
			if (value[0].equals("M")) 
			{
				hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
			} 
			else 
			{
				hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
			}
		}
		
		int p = Integer.parseInt(outputReduce.getConfiguration().get("p"));
		float result = 0.0f;
		float m_ij;
		float n_jk;
		for (int j = 0; j < p; j++) 
		{
			m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
			n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
			result += m_ij * n_jk;
		}
		if (result != 0.0f) 
		{
			Text t = new Text();
			t.set("Null con cac");
			outputReduce.write(t, new Text(key.toString() + "," + Float.toString(result)));
		}
	}
}//class MatrixMultiplyReduce
 
public class MatrixMultiply extends Configured implements Tool
{
	
	@Override
	public int run(String[] arg0) throws Exception 
	{
		
		Configuration conf = new Configuration();
		
		conf.set("m", "2");
		conf.set("n", "2");
		conf.set("p", "3");
		
		Job job = Job.getInstance(conf, "Matrix Multiply");
		
		job.setJarByClass(MatrixMultiply.class); 
		job.setMapperClass(MatrixMultiplyMapper.class);
		job.setReducerClass(MatrixMultiplyReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);   
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
  
	public static void main(String[] args) throws Exception 
	{        
                       
		MatrixMultiply runner = new MatrixMultiply();
		int exitcode = ToolRunner.run(runner, args);
		System.exit(exitcode);
       
    }//main

}//MatrixMultiply
