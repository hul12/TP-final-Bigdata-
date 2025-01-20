package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJob2 extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    public void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        boolean isRelated = false;

        for(IntWritable val : values) {
            if(val.get() == 0) {
                isRelated = true;
                break;
            }

            counter += val.get();
        }

        if(counter > 0 && !isRelated) {
            context.write(key, new IntWritable(counter));
        }
    }
}
