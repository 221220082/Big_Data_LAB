import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TMallDataCleaning {

    /**
     * Mapper类：负责解析CSV格式的原始数据并进行清洗
     */
    public static class DataCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // 跳过CSV文件可能的标题行
            if (key.get() == 0 && value.toString().contains("user_id")) {
                return;
            }
            
            // 使用CSV解析逻辑
            String[] fields = parseCSVLine(value.toString());
            if (fields.length != 6) {
                context.getCounter("Invalid Records", "Wrong Field Number").increment(1);
                return;
            }
            
            // 解析基础字段
            String user_id = fields[0].trim();
            String age_range = parseAge(fields[1].trim());
            String gender = parseGender(fields[2].trim());
            String merchant_id = fields[3].trim();
            String label = fields[4].trim();
            String activity_log = fields[5].trim();
            
            // 验证必要字段
            if (user_id.isEmpty() || merchant_id.isEmpty()) {
                context.getCounter("Invalid Records", "Missing Required Fields").increment(1);
                return;
            }
            
            // 输出用户基本信息（包含label）
            outputKey.set(user_id);
            outputValue.set("INFO:" + age_range + "," + gender + "," + label);
            context.write(outputKey, outputValue);
            
            // 解析并输出行为日志（将在Reducer中添加label）
            if (!activity_log.isEmpty()) {
                String[] activities = activity_log.split("#");
                for (String activity : activities) {
                    String[] parts = activity.split(":");
                    if (parts.length == 5) {
                        String item_id = parts[0].trim();
                        String category_id = parts[1].trim();
                        String brand_id = parts[2].trim();
                        String time = parts[3].trim();
                        String action_type = parts[4].trim();
                        
                        // 验证行为日志字段
                        if (!item_id.isEmpty() && !category_id.isEmpty() 
                                && !brand_id.isEmpty() && !time.isEmpty() 
                                && !action_type.isEmpty()) {
                            outputValue.set("LOG:" + item_id + "," + category_id + "," + 
                                          merchant_id + "," + brand_id + "," + time + "," + action_type);
                            context.write(outputKey, outputValue);
                        } else {
                            context.getCounter("Invalid Activities", "Missing Activity Fields").increment(1);
                        }
                    } else {
                        context.getCounter("Invalid Activities", "Malformed Activity").increment(1);
                    }
                }
            }
        }
        
        private String[] parseCSVLine(String line) {
            line = line.replaceAll("\",\"", "\u0001");
            String[] fields = line.split(",");
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].replace('\u0001', ',');
                fields[i] = fields[i].replaceAll("^\"|\"$", "");
            }
            return fields;
        }
        
        private String parseAge(String age) {
            if (age.equalsIgnoreCase("NULL") || age.equals("0")) {
                return "unknown";
            }
            return age;
        }
        
        private String parseGender(String gender) {
            if (gender.equalsIgnoreCase("NULL") || gender.equals("2")) {
                return "unknown";
            }
            return gender;
        }
    }

    /**
     * Reducer类：整理数据并输出到不同文件，在行为日志中添加label
     */
    public static class DataCleaningReducer extends Reducer<Text, Text, NullWritable, Text> {
        
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            String age_range = "unknown";
            String gender = "unknown";
            String label = "unknown";  // 初始化label
            List<String> activities = new ArrayList<>();
            
            for (Text val : values) {
                String[] parts = val.toString().split(":", 2);
                if (parts.length < 2) {
                    context.getCounter("Invalid Records", "Malformed Value").increment(1);
                    continue;
                }
                
                if (parts[0].equals("INFO")) {
                    String[] info = parts[1].split(",");
                    if (info.length >= 3) {  // 现在INFO包含age_range,gender,label
                        age_range = info[0];
                        gender = info[1];
                        label = info[2];  // 获取label值
                    } else {
                        context.getCounter("Invalid Records", "Malformed INFO").increment(1);
                    }
                } else if (parts[0].equals("LOG")) {
                    activities.add(parts[1]);
                }
            }
            
            // 输出用户信息文件（格式不变）
            multipleOutputs.write("UserInfo", NullWritable.get(), 
                    new Text(key.toString() + ":" + age_range + "," + gender));
            
            // 输出行为日志文件（每行末尾添加label）
            for (String activity : activities) {
                multipleOutputs.write("UserBehavior", NullWritable.get(), 
                        new Text(key.toString() + ":" + activity + "," + label));  // 添加label
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TMallDataCleaning <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        conf.set("textinputformat.record.delimiter", "\n");
        
        Job job = Job.getInstance(conf, "TMall Data Cleaning With Label");
        job.setJarByClass(TMallDataCleaning.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        MultipleOutputs.addNamedOutput(job, "UserInfo", 
            TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "UserBehavior", 
            TextOutputFormat.class, NullWritable.class, Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
