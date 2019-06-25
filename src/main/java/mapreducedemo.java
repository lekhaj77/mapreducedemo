import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class mapreducedemo {


    public class mapper extends Mapper<LongWritable, Text,Text, IntWritable> {

        Text outkey = new Text();
        IntWritable outvalue = new IntWritable();

        public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException {


            String [] words = value.toString().split(",");

            for ( String word : words ){

                outkey.set(word);
                outvalue.set(1);
                con.write(outkey,outvalue);


            }
        }
    }




        public class reducerdemo extends Reducer<Text, IntWritable,Text,IntWritable> {

            IntWritable outvalue = new IntWritable();

            public void reducer(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

                int sum =0 ;

                for ( IntWritable value : values){

                    sum = sum+value.get();


                }

                outvalue.set(sum);

                context.write(key,outvalue);
            }

        }




        public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf,"MapReduceDemo");
            job.setJarByClass(mapreducedemo.class);

            job.setMapperClass(mapper.class);
            job.setReducerClass(reducerdemo.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true)?0:1);

        }
    }


