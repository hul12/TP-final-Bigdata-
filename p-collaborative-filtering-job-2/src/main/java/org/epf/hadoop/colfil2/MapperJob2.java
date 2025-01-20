package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MapperJob2 extends Mapper<LongWritable, Text, UserPair, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        String userID = parts[0];
        String[] relations = parts[1].split(",");
        List<String> relationsList = Arrays.asList(relations);

        for (int i = 0; i < relationsList.size(); i++) {
            for (int j = i + 1; j < relationsList.size(); j++) {
//                String[] relationPair = {relations[i], relations[j]};
//                Arrays.sort(relationPair);
                UserPair pairKey = new UserPair(relations[i], relations[j]);
                context.write(pairKey, new IntWritable(1));
            }
        }

        for (String s : relationsList) {
//            String[] relationPair = {s, userID};
//            Arrays.sort(relationPair);
            UserPair pairKey = new UserPair(s, userID);
            context.write(pairKey, new IntWritable(0));
        }
    }
}