import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class BrandAnalysis {

    // 自定义Writable类用于排序 (保持不变)
    public static class BrandStats implements WritableComparable<BrandStats> {
        private double repurchaseRate;
        private int purchase;
        private int cart;
        private int collect;
        private int click;

        public BrandStats() {}

        public BrandStats(double repurchaseRate, int purchase, int cart, int collect, int click) {
            this.repurchaseRate = repurchaseRate;
            this.purchase = purchase;
            this.cart = cart;
            this.collect = collect;
            this.click = click;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(repurchaseRate);
            out.writeInt(purchase);
            out.writeInt(cart);
            out.writeInt(collect);
            out.writeInt(click);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            repurchaseRate = in.readDouble();
            purchase = in.readInt();
            cart = in.readInt();
            collect = in.readInt();
            click = in.readInt();
        }

        @Override
        public int compareTo(BrandStats other) {
            // 按复购率降序
            if (this.repurchaseRate != other.repurchaseRate) {
                return Double.compare(other.repurchaseRate, this.repurchaseRate);
            }
            // 按购买总数降序
            if (this.purchase != other.purchase) {
                return Integer.compare(other.purchase, this.purchase);
            }
            // 按购物车数量降序
            if (this.cart != other.cart) {
                return Integer.compare(other.cart, this.cart);
            }
            // 按收藏数量降序
            if (this.collect != other.collect) {
                return Integer.compare(other.collect, this.collect);
            }
            // 按点击数量降序
            return Integer.compare(other.click, this.click);
        }
        
        @Override
        public String toString() {
            return String.format("%.2f,%d,%d,%d,%d", repurchaseRate, purchase, cart, collect, click);
        }
    }

    // 第一阶段Mapper：解析原始数据 (保持不变)
    public static class BrandMapper extends Mapper<Object, Text, Text, Text> {
        private Text brandId = new Text();
        private Text action = new Text();

        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(":");
            if (parts.length < 2) return;
            
            String[] logData = parts[1].split(",");
            if (logData.length < 7) return;
            
            String brand_id = logData[3].trim();
            String action_type = logData[5].trim();
            String label = logData[6].trim();
            
            if (!brand_id.isEmpty()) {
                brandId.set(brand_id);
                action.set(action_type + "," + label);
                context.write(brandId, action);
            }
        }
    }

    // 修改后的第一阶段Reducer：输出BrandStats作为键
    public static class BrandReducer extends Reducer<Text, Text, BrandStats, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            int purchase = 0;
            int cart = 0;
            int collect = 0;
            int click = 0;
            int repurchase = 0;
            int totalPurchase = 0;
            
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length < 2) continue;
                
                String action_type = parts[0];
                String label = parts[1];
                
                switch (action_type) {
                    case "0": click++; break;
                    case "1": cart++; break;
                    case "2": 
                        purchase++;
                        totalPurchase++;
                        if ("1".equals(label)) repurchase++;
                        break;
                    case "3": collect++; break;
                }
            }
            
            double repurchaseRate = totalPurchase > 0 ? 
                Math.round((double)repurchase/totalPurchase * 100)/100.0 : 0.0;
            
            BrandStats stats = new BrandStats(repurchaseRate, purchase, cart, collect, click);
            context.write(stats, key); // 注意这里交换了键值位置
        }
    }

    // 修改后的第二阶段Mapper：直接传递键值对
    public static class ResultMapper extends Mapper<BrandStats, Text, BrandStats, Text> {
        public void map(BrandStats key, Text value, Context context) 
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    // 添加第二阶段Reducer：保持排序顺序
    public static class ResultReducer extends Reducer<BrandStats, Text, Text, Text> {
        private Text result = new Text();
        
        public void reduce(BrandStats key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(key.toString());
                context.write(val, result); // 品牌ID作为键，统计信息作为值
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // 第一轮Job：计算品牌统计并输出为SequenceFile
        Job job1 = Job.getInstance(conf, "Brand Analysis - Phase 1");
        job1.setJarByClass(BrandAnalysis.class);
        job1.setMapperClass(BrandMapper.class);
        job1.setReducerClass(BrandReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(BrandStats.class);
        job1.setOutputValueClass(Text.class);
        
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        Path tempOutput = new Path("temp_output");
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempOutput);
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // 第二轮Job：排序并格式化输出
        Job job2 = Job.getInstance(conf, "Brand Analysis - Phase 2");
        job2.setJarByClass(BrandAnalysis.class);
        job2.setMapperClass(ResultMapper.class);
        job2.setReducerClass(ResultReducer.class);
        job2.setNumReduceTasks(1); // 单个Reducer确保全局排序
        
        // 设置输入输出类型
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapOutputKeyClass(BrandStats.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
