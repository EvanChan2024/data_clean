import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import time
import taos
import schedule
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pandas as pd
'''
ver3.5
优化程序日志记录，新增日志轮转功能
新增梁端位移、裂缝清洗
'''

# 锁
mqtt_lock = threading.Lock()

# 创建一个日志记录器
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# 指定日志文件的路径
log_directory = r'E:\sqlite\v3_series\v3.5\log_nd01'
if not os.path.exists(log_directory):
    os.makedirs(log_directory)  # 如果目录不存在，则创建

log_file = os.path.join(log_directory, 'my_log.log')
file_handler = TimedRotatingFileHandler(
    log_file, when='H', interval=3, backupCount=56
)
file_handler.setLevel(logging.INFO)

# 定义日志消息的格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 将处理器添加到日志记录器中
logger.addHandler(file_handler)


def job2(string, sensorcode, thread_index, cycle):
    def read_data(statement):
        try:
            result_list = []
            conn: taos.TaosConnection = taos.connect(host="jkjc1",
                                                     user="root",
                                                     password="taosdata",
                                                     database="db_jkjc",
                                                     port=6030)
            result1 = conn.query(statement)
            thread_id = check_current_thread()
            active_threads = check_active_threads()
            logger.info(f"{thread_id}: Connected to database, active threads: %d", len(active_threads))
            result_list = result1.fetch_all()  # 将查询结果转换为列表
            thre = threshold_cal(result_list)
            logger.info(f"{thread_id}: Threshold ready {thre}, Query: {statement}")
            conn.close()
            return thre
        except taos.Error as e:
            if "Table does not exist" in str(e):  # Adjust the condition to match the specific error message or code
                logger.warning(f"Table not found, returning empty result list. Query: {statement}")
                thre_2 = 100
                logger.info(f"Threshold ready {thre_2}")
            else:
                logger.error(f"Connecting to database failed: {e}, query: {statement}")
                thre_2 = 100
                logger.info(f"Threshold ready {thre_2}")
            return thre_2
        except Exception as e:
            logger.error(f"An error occurred: {e}, query: {statement}")

    def threshold_cal(data):
        try:
            q75 = np.percentile(data, 99) + (np.percentile(data, 99) - np.percentile(data, 1)) * 10  # 先将data转为numpy数组
            q25 = np.percentile(data, 1) - (np.percentile(data, 99) - np.percentile(data, 1)) * 10
            # 避免计算max时出现异常
            thresholds = [np.abs(q75), np.abs(q25)]
            threshold = max(thresholds) if thresholds else 100
            if np.isnan(threshold):
                threshold = 100

        except Exception as e:
            logger.error(f"Threshold calculating failed: {e}, data: {data}")
            threshold = 100
        return threshold

    def check_current_thread():
        current_thread = threading.current_thread()
        thread_id = current_thread.ident
        return thread_id

    def check_active_threads():
        active_threads = []
        for thread in threading.enumerate():
            if thread.is_alive():
                active_threads.append(thread.name)
        return active_threads

    def job3():
        global shared_value
        result2 = read_data(string)
        shared_value[thread_index] = result2

    # 定时任务
    try:
        interval_in_seconds = cycle
        schedule.every(interval_in_seconds).seconds.do(job3)  # 循环体放在这会导致单个线程中的多次循环打印相同值
        thread_id = check_current_thread()
        logger.info(f"{thread_id}: Scheduled Task On")
    except Exception as e:
        logger.error(f"Schedule task failed: {e}")


def job(topic1, mqtt_client_id_source, mqtt_client_id_destination, thread_index):
    source_broker_address = "221.226.48.78"
    source_port = 1885
    source_mqtt_username = 'jsti_jkjc'
    source_mqtt_password = 'Bridge321'
    destination_broker_address = "10.30.30.249"
    destination_port = 1883
    destination_mqtt_username = 'jsti'
    destination_mqtt_password = 'Bridge321'

    def on_connect(client, userdata, flags, rc):
        # 订阅主题
        client.subscribe(topic1)

    # MQTT 消息到来时的回调函数
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        try:
            if message:
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Connected to MQTT broker {topic1}")
            else:
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Connection failed")
            payload = message.payload
            unpack(payload)
        except Exception as e:
            logger.error(f"Data subscribing failed: {e}, message: {message}")

    def unpack(payload):
        # 解析二进制数据
        try:
            data = struct.unpack(">HBBBBB" + "f" * int(((len(payload)) - 7) / 4), payload)  # 解析后的二进制数据
            payload_len = int(((len(payload)) - 7) / 4)
            data_num = np.array(data[6:payload_len + 6], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
            thread_current = check_current_thread()
            logger.info(f"{thread_current}: Received data {data_num}")
            pack(data, payload_len)
        except Exception as e:
            logger.error(f"Data unpacking failed: {e}, payload: {payload}")

    def pack(data, payload_len):
        try:
            year = data[0]
            month = data[1]
            day = data[2]
            hour = data[3]
            minute = data[4]
            second = data[5]
            global shared_value
            data_num = np.array(data[6:payload_len + 6], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
            processed_data = process_data(data_num, shared_value[thread_index])
            cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second,
                                       *processed_data)
            thread_current = check_current_thread()
            logger.info(f"{thread_current}: Publishing data ready {processed_data}")
            mqtt_publish(destination_client, topic1, cleaned_data)
            # 检查活跃线程和异常中断的原因
            thread_exceptions = check_thread_exceptions()
            if thread_exceptions:
                for thread_name, exception in thread_exceptions.items():
                    logger.error(f"Thread Exceptions: {thread_name}: {exception}")
        except Exception as e:
            logger.error(f"Data packing failed: {e}, Topic: {topic1}, Data: {data}")

    def process_data(data, value):
        try:
            # 统计超过参考值的索引
            exceed_indices = np.where(np.abs(data) > value)[0]
            # 遍历超过参考值的索引
            for idx in exceed_indices:
                # data[idx] = value * np.random.uniform(0.1, 0.2)
                data[idx] = np.nan
            return data
        except Exception as e:
            logger.error(f"Data filter failed: {e}, data: {data}, value: {value}")

    def mqtt_publish(client, mqtt_topic, record_data):
        with mqtt_lock:
            try:
                thread_current = check_current_thread()
                active_threads = check_active_threads()
                logger.info(f"{thread_current}: Active Threads: %d", len(active_threads))
                client.publish(mqtt_topic, record_data, qos=1)
                logger.info(f"{thread_current}: Published to {mqtt_topic}")
            except Exception as e:
                logger.error(f"MQTT publish failed: {e}")

    def on_disconnect(client, userdata, rc):
        while rc != 0:
            thread_current = check_current_thread()
            logger.info(f"{thread_current}: Unexpected disconnection. Will try to reconnect... ({rc})")
            try:
                time.sleep(10)  # 设置重连尝试的间隔，例如等待10秒
                client.reconnect()
                rc = 0  # 如果成功重连，将 rc 设为 0 以退出循环
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Reconnection successful")
            except Exception as e:
                logger.error(f"{thread_current}: Reconnection failed: {e}")

    def check_current_thread():
        current_thread = threading.current_thread()
        thread_id = current_thread.ident
        return thread_id

    # 定义一个函数来检查活跃的线程
    def check_active_threads():
        active_threads = []
        for thread in threading.enumerate():
            if thread.is_alive():
                active_threads.append(thread.name)
        return active_threads

    # 定义一个函数来检查线程异常中断的原因
    def check_thread_exceptions():
        thread_exceptions = {}
        for thread in threading.enumerate():
            if not thread.is_alive() and hasattr(thread, 'exception'):
                thread_exceptions[thread.name] = thread.exception
        return thread_exceptions

    # 初始化客户端
    source_client = mqtt.Client(client_id=mqtt_client_id_source, clean_session=True)  # 一个线程一个client_id即可
    destination_client = mqtt.Client(client_id=mqtt_client_id_destination, clean_session=True)  # 一个线程一个client_id即可
    # 设置连接参数
    source_client.username_pw_set(username=source_mqtt_username, password=source_mqtt_password)
    destination_client.username_pw_set(username=destination_mqtt_username, password=destination_mqtt_password)

    # 绑定连接事件处理函数
    source_client.on_connect = on_connect
    destination_client.on_connect = on_connect

    # 绑定接收消息事件处理函数
    source_client.on_message = on_message

    # 绑定断开连接事件处理函数
    source_client.on_disconnect = on_disconnect
    destination_client.on_disconnect = on_disconnect

    # 连接 MQTT 服务器
    destination_client.connect(destination_broker_address, destination_port)  # 先连接destination
    source_client.connect(source_broker_address, source_port)

    # 循环监听消息
    source_client.loop_start()  # 这里是forever就错了
    destination_client.loop_forever()


if __name__ == "__main__":
    df = pd.read_excel(r'E:\sqlite\v3_series\v3.5\sensorinfo_part.xlsx', sheet_name='BRIDGE_TEST_SELFCHECK.T_BRIDGE')
    filtered_data = df[df['SENSOR_SUB_TYPE_NAME'].isin(['竖向位移', '主梁竖向位移', '主梁竖向位移监测', '主梁位移']) & df['SENSOR_POSITION'].isin(['连云港', '南京', '宿迁'])
        ][['FOREIGN_KEY', 'SENSOR_CODE']]
    bridge = filtered_data['FOREIGN_KEY'].to_list()
    sensor = filtered_data['SENSOR_CODE'].to_list()
    timecycle = [3600] * len(sensor)
    point = [5*3600*24] * len(sensor)
    # 共享变量，用于传递数值
    shared_value = [100] * len(sensor)
    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + bridge[i] + "/" + sensor[i]
        mqtt_client_id = "clean_1_" + bridge[i] + "_" + sensor[i]
        mqtt_client_id2 = "clean2_1_" + bridge[i] + "_" + sensor[i]
        string = 'select val from ' + '`' + bridge[i] + '-' + sensor[i] + '`' + ' order by ts desc' + ' limit ' + str(point[i])
        # 创建线程
        thread2 = threading.Thread(target=job2, args=(string, sensor[i], i, timecycle[i]))
        thread1 = threading.Thread(target=job, args=(topic, mqtt_client_id, mqtt_client_id2, i))
        threads.append(thread2)  # 将线程对象添加到列表中
        threads.append(thread1)  # 将线程对象添加到列表中
        thread2.start()  # 启动线程，注意thread1与thread2的顺序
        thread1.start()

    # 等待所有线程创建完成
    for thread2 in threads:  # 循环体放在这不会导致单个线程中的多次循环打印相同值
        thread2.join()
        while True:
            schedule.run_pending()
            time.sleep(1)
    for thread1 in threads:
        thread1.join()
