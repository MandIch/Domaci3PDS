package domaci3hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProsecnaTempDom {

    public static class AvgTuple implements Writable {

        private int minimalCounter = 0;
        private int maximalniCounter = 0;
        private int minimalnaSuma = 0;
        private int maximalnaSuma = 0;
        
        public int getMinimalCounter() {
			return minimalCounter;
		}
		public void setMinimalCounter(int minimalCounter) {
			this.minimalCounter = minimalCounter;
		}
		public int getMaximalniCounter() {
			return maximalniCounter;
		}
		public void setMaximalniCounter(int maximalniCounter) {
			this.maximalniCounter = maximalniCounter;
		}
		public int getMinimalnaSuma() {
			return minimalnaSuma;
		}
		public void setMinimalnaSuma(int minimalnaSuma) {
			this.minimalnaSuma = minimalnaSuma;
		}
		public int getMaximalnaSuma() {
			return maximalnaSuma;
		}
		public void setMaximalnaSuma(int maximalnaSuma) {
			this.maximalnaSuma = maximalnaSuma;
		}
		
		
		@Override
        public void readFields(DataInput in) throws IOException {
			minimalCounter = in.readInt();
			maximalniCounter = in.readInt();
			minimalnaSuma = in.readInt();
            maximalnaSuma = in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException {
        	out.writeInt(minimalCounter);
        	out.writeInt(maximalniCounter);
        	out.writeInt(minimalnaSuma);
        	out.writeInt(maximalnaSuma);
        }

        public String toString() {
            return "Prosek minimuma: " + (1.0 * minimalnaSuma/minimalCounter) + ", Prosek maksimuma: " + (1.0 * maximalnaSuma/maximalniCounter);
        }

    }

    public static class TempMapper extends Mapper<Object, Text, Text, AvgTuple> {
        private Text month = new Text();
        private AvgTuple outTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            month.set(line[1].substring(4,6));
            int temperature = Integer.parseInt(line[3]);
            String extreme = line[2];
            if(extreme.equals("TMIN")){
                outTuple.setMinimalnaSuma(temperature);
                outTuple.setMinimalCounter(1);
            }else if(extreme.equals("TMAX")){
                outTuple.setMaximalnaSuma(temperature);
                outTuple.setMaximalniCounter(1);
            }
            context.write(month, outTuple);
        }
    }

    public static class TempReducer extends Reducer<Text, AvgTuple, Text, AvgTuple> {

        private AvgTuple resultTuple = new AvgTuple();

        public void reduce(Text key, Iterable<AvgTuple> tuples, Context context) throws IOException, InterruptedException {
            
            int minimalniCounter = 0;
            int maximalniCounter = 0;
            int minimalnaSuma = 0;
            int maximalnaSuma = 0;

            for(AvgTuple tup : tuples){
            	minimalniCounter += tup.getMinimalCounter();
            	maximalniCounter += tup.getMaximalniCounter();
            	minimalnaSuma += tup.getMinimalnaSuma();
            	maximalnaSuma += tup.getMaximalnaSuma();
            }
            resultTuple.setMinimalCounter(minimalniCounter);
            resultTuple.setMaximalniCounter(maximalniCounter);
            resultTuple.setMinimalnaSuma(minimalnaSuma);
            resultTuple.setMaximalnaSuma(maximalnaSuma);
            context.write(key, resultTuple);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "prosek ekstremne temp");
        job.setJarByClass(ProsecnaTempDom.class);
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempReducer.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}