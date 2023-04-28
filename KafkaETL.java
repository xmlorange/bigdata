import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.Context;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;

public class KafkaETL {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaBrokers = parameterTool.get("kafkaBrokers", "bigdata03:9092");
        String kafkaTopic = parameterTool.get("kafkaTopic", "data");
        String redisHost = parameterTool.get("redisHost", "bigdata03");
        int redisPort = parameterTool.getInt("redisPort", 6379);

        // 读取国家地区信息数据
        DataStream<HashMap<String, String>> countries = env.addSource(new RedisSource(redisHost, redisPort));

        // 从 Kafka 中读取数据，并进行清洗、拆分
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), parameterTool.getProperties());
        DataStream<String> kafkaData = env.addSource(kafkaConsumer);
        DataStream<JSONObject> cleanedData = kafkaData.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject record = JSON.parseObject(value);
                String countryCode = record.getString("countryCode");
                JSONArray dataArray = record.getJSONArray("data");
                for (int i = 0; i < dataArray.size(); i++) {
                    JSONObject newData = new JSONObject();
                    newData.putAll(record);
                    newData.putAll(dataArray.getJSONObject(i));
                    newData.remove("data");
                    newData.put("countryCode", countryCode);
                    out.collect(newData);
                }
            }
        });

        // 将国家代码转换为大区代码
        DataStream<JSONObject> transformedData = cleanedData.connect(countries)
                .flatMap(new FlatMapFunction<Tuple2<JSONObject, HashMap<String, String>>, JSONObject>() {
                    @Override
                    public void flatMap(Tuple2<JSONObject, HashMap<String, String>> value, Collector<JSONObject> out) throws Exception {
                        JSONObject record = value.f0;
                        HashMap<String, String> countryInfo = value.f1;
                        String countryCode = record.getString("countryCode");
                        String areaCode = null;
                        for (Map.Entry<String, String> entry : countryInfo.entrySet()) {
                            String[] countries = entry.getValue().split(",");
                            if (Arrays.asList(countries).contains(countryCode)) {
                                areaCode = entry.getKey();
                                break;
                            }
                        }
                        record.put("countryCode", areaCode);
                        out.collect(record);
                    }
                });

        // 将处理后的数据写回到 Kafka 中
        transformedData.map(record -> record.toJSONString()).forEach(jsonString -> {
            producer.send(new ProducerRecord<>("etldata", jsonString));
        });

    // 关闭 Kafka 生产者
        producer.close();
    }
}
