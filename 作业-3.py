from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table.descriptors import Schema, Json, Kafka, FileSystem
from pyflink.table.udf import ScalarFunction
from pyflink.table.window import Tumble
from pyflink.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.util import OutputTag
from pyflink.table import EnvironmentSettings
from pyflink.table.expressions import col
from datetime import datetime, timedelta
import json


# 定义自定义的UDF，用于从JSON字符串中提取字段值
class ExtractField(ScalarFunction):
    def eval(self, json_str, field):
        import json
        try:
            json_dict = json.loads(json_str)
            return json_dict.get(field, None)
        except:
            return None

# 设置Flink执行环境
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# 定义Kafka消费者
kafka_consumer = FlinkKafkaConsumer(
    'user_behavior_topic',  # Kafka topic名称
    Json(),  # 数据格式为JSON
    properties={
        'bootstrap.servers': 'localhost:9092',  # Kafka的bootstrap servers地址
        'group.id': 'user_behavior_group',  # 消费者组ID
        'auto.offset.reset': 'latest'  # 从最新的偏移量开始消费
    })

# 从Kafka消费数据，注册表
t_env.connect(kafka_consumer) \
    .with_format(Json().fail_on_missing_field(False)) \
    .with_schema(Schema()
                 .field("userId", Types.STRING())
                 .field("itemId", Types.STRING())
                 .field("categoryId", Types.STRING())
                 .field("behavior", Types.STRING())
                 .field("timestamp", Types.STRING())) \
    .in_append_mode() \
    .register_table("user_behavior")

# 注册自定义UDF
t_env.create_temporary_function("extract_field", ExtractField())

# 需求一：热门商品PV统计
t_env.sql_query("""
    SELECT itemId, COUNT(*) as pv
    FROM user_behavior
    WHERE behavior = 'pv'
      AND TIMESTAMPDIFF(SECOND, TO_TIMESTAMP(timestamp), CURRENT_TIMESTAMP()) <= 3600
    GROUP BY itemId
    ORDER BY pv DESC
    LIMIT 10
""").execute().print()

# 需求二：页面浏览数统计
t_env.sql_query("""
    SELECT extract_field(url, 'url') as url, COUNT(*) as view_count
    FROM user_behavior
    WHERE behavior = 'pv'
      AND TIMESTAMPDIFF(SECOND, TO_TIMESTAMP(timestamp), CURRENT_TIMESTAMP()) <= 600
    GROUP BY extract_field(url, 'url')
    ORDER BY view_count DESC
    LIMIT 10
""").execute().print()

# 执行Flink作业
env.execute("Real-time Data Processing")


# 需求三 恶意登录监控

# 定义Kafka连接相关配置
kafka_props = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-kafka-demo-group"
}
kafka_source = FlinkKafkaConsumer(
    "user_behavior",
    SimpleStringSchema(),
    kafka_props
)

# 添加Kafka数据源到TableEnvironment
t_env.connect(
        Kafka()
        .version("universal")
        .topic("user_behavior")
        .start_from_latest()
        .properties(**kafka_props)
    ) \
    .with_format(Json().json_schema("{"
                                    "  'type': 'object',"
                                    "  'properties': {"
                                    "    'userId': {'type': 'string'},"
                                    "    'itemid': {'type': 'string'},"
                                    "    'categoryId': {'type': 'string'},"
                                    "    'behavior': {'type': 'string'},"
                                    "    'timestamp': {'type': 'string'}"
                                    "  }"
                                    "}")) \
    .with_schema(Schema()
                 .field("userId", DataTypes.STRING())
                 .field("itemid", DataTypes.STRING())
                 .field("categoryId", DataTypes.STRING())
                 .field("behavior", DataTypes.STRING())
                 .field("timestamp", DataTypes.STRING())) \
    .create_temporary_table("user_behavior")

# 定义UDF，用于判断是否恶意登录
@udf(input_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()],
     result_type=DataTypes.BOOLEAN())
def is_malicious_login(ip, userId, eventTime, method):
    if method != 'POST':
        return False
    login_success = t_env.from_path("access_logs") \
        .select("userId, eventTime, url") \
        .where(f"url = '/login' and userId = '{userId}' and eventTime >= '{eventTime}' - INTERVAL '2' SECOND") \
        .execute() \
        .collect()
    return len(login_success) < 2

# 添加Kafka数据源到TableEnvironment
t_env.connect(
        Kafka()
        .version("universal")
        .topic("access_logs")
        .start_from_latest()
        .properties(**kafka_props)
    ).with_format(Json().json_schema("{"
                                    "  'type': 'object',"
                                    "  'properties': {"
                                    "    'ip': {'type': 'string'},"
                                    "    'userId': {'type': 'string'},"
                                    "    'eventTime': {'type': 'string'},"
                                    "    'method': {'type': 'string'},"
                                    "    'url': {'type': 'string'}"
                                    "  }"
                                    "}")).with_schema(Schema()
                 .field("ip", DataTypes.STRING())
                 .field("userId", DataTypes.STRING())
                 .field("eventTime", DataTypes.STRING()).field("method", DataTypes.STRING())
                 .field("url", DataTypes.STRING())).create_temporary_table("access_logs")

# 查询是否存在恶意登录，并将结果输出到控制台
t_env.from_path("access_logs") \
    .join(
        t_env.from_path("user_behavior"),
        "userId = userId"
    ).select("ip, userId, eventTime, method") \
    .where("is_malicious_login(ip, userId, eventTime, method) = True") \
    .execute() \
    .print()
    
# 执行流处理任务
env.execute("malicious_login_monitor")


# 需求四  实时订单支付监控

class OrderExpiredProcessFunction(ProcessFunction):
    def __init__(self):
        self.order_state = None
    
    def open(self, runtime_context):
        order_state_desc = ValueStateDescriptor("order_state", str)
        self.order_state = runtime_context.get_state(order_state_desc)
    
    def process_element(self, value, ctx, out):
        order_info = json.loads(value)
        order_key = (order_info['userId'], order_info['itemid'])
        last_order_time = datetime.strptime(order_info['timestamp'], '%Y-%m-%d %H:%M:%S')
        if self.order_state.value() is None:
            # 第一次收到订单信息，更新状态为当前订单信息
            self.order_state.update(json.dumps(order_info))
            # 注册定时器，在订单失效时间点触发
            ctx.timer_service().register_event_time_timer(last_order_time + timedelta(minutes=15))
        else:
            # 已经存在订单信息，比较时间戳，更新状态为最新订单信息
            last_order_info = json.loads(self.order_state.value())
            last_order_time = datetime.strptime(last_order_info['timestamp'], '%Y-%m-%d %H:%M:%S')
            if last_order_time < last_order_time:
                self.order_state.update(json.dumps(order_info))
                # 取消之前的定时器，重新注册定时器
                ctx.timer_service().delete_event_time_timer(last_order_time + timedelta(minutes=15))
                ctx.timer_service().register_event_time_timer(last_order_time + timedelta(minutes=15))
    
    def on_timer(self, timestamp, ctx, out):
        # 定时器触发，订单已经失效，输出到侧输出流
        expired_order_info = json.loads(self.order_state.value())
        out.collect(expired_order_info)
        # 清空状态
        self.order_state.clear()

# 定义自定义函数，计算订单总金额
class TotalAmount(ScalarFunction):
    def eval(self, behavior, amount):
        if behavior == 'buy':
            return amount
        else:
            return 0

# 初始化Flink环境
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
env = StreamExecutionEnvironment.get_execution_environment()
t_env = env.get_table_environment()

# 从Kafka消费购买行为数据
consumer_properties = {'bootstrap.servers': 'localhost:9092', 'group.id': 'group1'}
consumer = FlinkKafkaConsumer('user_behavior', SimpleStringSchema(), consumer_properties)
user_behavior_stream = env.add_source(consumer)

# 将JSON格式的数据转换为Table
user_behavior_table = t_env.from_data_stream(user_behavior_stream, ['userId', 'itemid', 'categoryId', 'behavior', 'timestamp'])

# 定义窗口，按照用户ID和商品ID进行分组
tumble_window = user_behavior_table.window(Tumble.over('15.minutes').on('timestamp').alias('w')) \
    .group_by('userId, itemid, categoryId, w')

# 计算订单总金额
t_env.register_function("total_amount", TotalAmount())
order_table = user_behavior_table.select('userId, itemid, total_amount(behavior, amount) as amount, timestamp') \
    .where('behavior == "buy"')

# 注册侧输出流
expired_orders = OutputTag('expired_orders')

# 处理订单过期事件
order_stream = order_table.select('userId, itemid, amount, timestamp') \
    .add_sink(t_env.from_path('order_sink')) \
    .process(OrderExpiredProcessFunction()) \
    .get_side_output(expired_orders) \
    .add_sink(t_env.from_path('expired_order_sink'))

# 输出订单总金额
result_table = order_table.window(Tumble.over('1.hour').on('timestamp').alias('w')) \
    .group_by('w') \
    .select('w.start as window_start, w.end as window_end, sum(amount) as total_amount')

# 输出到控制台
t_env.execute_sql('CREATE TABLE console_sink (window_start TIMESTAMP(3), window_end TIMESTAMP(3), total_amount DOUBLE) '
                  'WITH ("connector"="print")')
result_table.insert_into('console_sink')

# 提交任务
env.execute()
