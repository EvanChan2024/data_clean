import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import time
import taos
import schedule
import logging
import pandas as pd
'''
ver3.1
'''

# 锁
mqtt_lock = threading.Lock()

# 创建一个日志记录器
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# 创建一个文件处理器来将日志写入到文件
file_handler = logging.FileHandler('app2.log')
file_handler.setLevel(logging.INFO)

# 定义日志消息的格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 将处理器添加到日志记录器中
logger.addHandler(file_handler)


def job2(string, sensorcode, thread_index, cycle):
    def read_data(statement):
        try:
            conn: taos.TaosConnection = taos.connect(host="dbmaster",
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
            logger.info(f"{thread_id}: Threshold ready {thre}")
            conn.close()
            return thre
        except Exception as e:
            logger.error(f"Connecting to database failed: {e}, query: {statement}",)

    def threshold_cal(data):
        try:
            # 将二维列表转换为 numpy 数组
            data_array = np.array(data)
            thresholds = []
            for col_data in data_array.T:  # 对数组的转置进行迭代，以便按列访问数据
                q25 = np.percentile(col_data, 5)
                q75 = np.percentile(col_data, 95)
                iqr = (q75 - q25) * 10
                q1 = q25 - iqr
                q2 = q75 + iqr
                threshold = max(np.abs(q1), np.abs(q2))
                thresholds.append(threshold)
            return thresholds
        except Exception as e:
            logger.error(f"Threshold calculating failed: {e}, data: {data}")

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
        for i in range(3):
            shared_value[3*(thread_index-1)+i] = result2[i]  # 3=len(data_num)

    # 定时任务
    try:
        interval_in_seconds = cycle
        schedule.every(interval_in_seconds).seconds.do(job3)  # 循环体放在这会导致单个线程中的多次循环打印相同值
        thread_id = check_current_thread()
        logger.info(f"{thread_id}: Scheduled Task On")
    except Exception as e:
        logger.error(f"Schedule task failed: {e}")


def job(topic1, topic1_new, mqtt_client_id1, thread_index):
    # MQTT 消息到来时的回调函数
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        try:
            if message:
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Connected to MQTT broker")
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
            processed_data =[]
            for i in range(len(data_num)):
                processed = process_data(data_num[i], shared_value[3*(thread_index-1)+i])  # 3=len(data_num)
                processed_data.append(processed)
            cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second,
                                       *processed_data)
            thread_current = check_current_thread()
            logger.info(f"{thread_current}: Publishing data ready {processed_data}")
            mqtt_publish(client, topic1_new, cleaned_data)
            # 检查活跃线程和异常中断的原因
            thread_exceptions = check_thread_exceptions()
            if thread_exceptions:
                for thread_name, exception in thread_exceptions.items():
                    logger.error(f"Thread Exceptions: {thread_name}: {exception}")
        except Exception as e:
            logger.error(f"Data packing failed: {e}, Topic: {topic1_new}, Data: {data}")

    def process_data(data, value):
        try:
            # 统计超过参考值的索引
            exceed_indices = np.where(np.abs(data) > value)[0]
            # 遍历超过参考值的索引
            for idx in exceed_indices:
                data[idx] = value * np.random.uniform(0.1, 0.2)
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
                time.sleep(180)  # 设置重连尝试的间隔，例如等待10秒
                client.reconnect()
                rc = 0  # 如果成功重连，将 rc 设为 0 以退出循环
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Reconnection successful")
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

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

    broker_address = "221.226.48.78"
    port = 1885
    mqtt_username = 'jsti_jkjc'
    mqtt_password = 'Bridge321'
    # 创建 MQTT 客户端实例
    client = mqtt.Client(client_id=mqtt_client_id1, clean_session=True)  # 一个线程一个client_id即可
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    # 设置消息到来时的回调函数
    client.on_message = on_message
    # 设置断开连接时的回调函数
    client.on_disconnect = on_disconnect
    # 连接 MQTT 服务器
    client.connect(broker_address, port)
    # 订阅主题
    client.subscribe(topic1)
    # 循环监听消息
    client.loop_forever()


if __name__ == "__main__":
    df = pd.read_excel(r'D:\gzwj\01.重点工作\sensorinfo.xlsx', sheet_name='BRIDGE_TEST_SELFCHECK.T_BRIDGE')
    filtered_data = df[df['SENSOR_SUB_TYPE_NAME'].isin(['应变/温度', '结构应变监测(振弦)', '应变温度', '结构应力'])][['FOREIGN_KEY', 'SENSOR_CODE']]
    bridge = filtered_data['FOREIGN_KEY'].to_list()
    sensor = filtered_data['SENSOR_CODE'].to_list()
    timecycle = [3600*24] * len(sensor)
    point = [144] * len(sensor)
    col = 3  # 一个包中的数据个数
    # 共享变量，用于传递数值
    shared_value = [300] * len(sensor) * col
    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + bridge[i] + "/" + sensor[i]
        topic_new = "cleandata/" + bridge[i] + "/" + sensor[i]
        mqtt_client_id = "test_" + bridge[i] + "_" + sensor[i]
        string = 'select val1,val2,val3 from ' + '`' + sensor[i] + '`' + ' order by ts desc' + ' limit ' + str(point[i])
        # 创建线程
        thread2 = threading.Thread(target=job2, args=(string, sensor[i], i, timecycle[i]))
        thread1 = threading.Thread(target=job, args=(topic, topic_new, mqtt_client_id, i))
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
