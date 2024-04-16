import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import time
import taos
import schedule
import logging
'''
实时在线滤除异常数据正式版
订阅-转换-处理-转换-发布
添加自动重连
'''

# 锁
mqtt_lock = threading.Lock()

# 创建一个日志记录器
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# 创建一个文件处理器来将日志写入到文件
file_handler = logging.FileHandler('app.log')
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
            result_list = result1.fetch_all()  # 将查询结果转换为列表
            # print(result_list)
            thre = threshold_cal(result_list)
            conn.close()
            return thre
        except Exception as e:
            logger.error(f"Connecting to database failed: {e}, query: {statement}",)

    def threshold_cal(data):
        try:
            q75 = np.percentile(data, 95) + (np.percentile(data, 95) - np.percentile(data, 5)) * 10  # 先将data转为numpy数组
            q25 = np.percentile(data, 5) - (np.percentile(data, 95) - np.percentile(data, 5)) * 10
            threshold = max(np.abs(q75), np.abs(q25))
            return threshold
        except Exception as e:
            logger.error(f"Threshold calculating failed: {e}, data: {data}")

    def job3():
        global shared_value
        result2 = read_data(string)
        shared_value[thread_index] = result2
        # print(shared_value)

    # job3()
    # 定时任务
    try:
        interval_in_seconds = cycle
        schedule.every(interval_in_seconds).seconds.do(job3)  # 循环体放在这会导致单个线程中的多次循环打印相同值
    except Exception as e:
        logger.error(f"Schedule task failed: {e}")


def job(topic1, topic1_new, mqtt_client_id1, thread_index):
    # MQTT 消息到来时的回调函数
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        try:
            topic2 = message.topic
            payload = message.payload
            unpack(payload)
            # print(topic2)
        except Exception as e:
            logger.error(f"Data subscribing failed: {e}, message: {message}")

    def unpack(payload):
        # 解析二进制数据
        try:
            data = struct.unpack(">HBBBBB" + "f" * int(((len(payload)) - 7) / 4), payload)  # 解析后的二进制数据
            payload_len = int(((len(payload)) - 7) / 4)
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
            data_num = np.array(data[6:payload_len + 5], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
            processed_data = process_data(data_num, shared_value[thread_index])
            # print(shared_value)
            cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second,
                                       *processed_data)
            mqtt_publish(client, topic1_new, cleaned_data)
            # 在你的代码中适当位置调用这两个函数来检查活跃的线程和异常中断的原因
            active_threads = check_active_threads()
            logger.info("Active Threads: %s", active_threads)
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
            # 初始化异常索引列表
            abnormal_indices = []
            # 遍历超过参考值的索引
            for idx in exceed_indices:
                # 记录孤立值
                if idx - 1 not in exceed_indices and idx + 1 not in exceed_indices:
                    abnormal_indices.append(idx)
                    left_neighbor = data[idx - 1]
                    data[idx] = left_neighbor
                # 记录连续两个异常值
                if idx - 1 not in exceed_indices and idx + 1 in exceed_indices and idx + 2 not in exceed_indices:
                    abnormal_indices.append(idx)
                    abnormal_indices.append(idx + 1)
                    left_neighbor = data[idx - 1]
                    right_neighbor = data[idx + 2]
                    data[idx] = left_neighbor
                    data[idx + 1] = right_neighbor
            return data
        except Exception as e:
            logger.error(f"Data filter failed: {e}, data: {data}, value: {value}")

    def mqtt_publish(client, mqtt_topic, record_data):
        with mqtt_lock:
            try:
                client.publish(mqtt_topic, record_data, qos=1)
                print(f"Published to {mqtt_topic}")
            except Exception as e:
                logger.error(f"MQTT publish failed: {e}")

    def on_reconnect(client, userdata, rc):
        while rc != 0:
            print(f"Unexpected disconnection. Will try to reconnect... ({rc})")
            try:
                time.sleep(10)  # 设置重连尝试的间隔，例如等待10秒
                client.reconnect()
                print("Reconnection successful")
                rc = 0  # 如果成功重连，将 rc 设为 0 以退出循环
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

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
    client.on_reconnect = on_reconnect
    # 连接 MQTT 服务器
    client.connect(broker_address, port)
    # 订阅主题
    client.subscribe(topic1)
    # 循环监听消息
    client.loop_forever()


if __name__ == "__main__":
    sensor = ['KGD-DIS-G02-001-01', 'KGD-DIS-G02-001-02', 'KGD-DIS-G02-001-03', 'KGD-DIS-G02-002-04', 'KGD-DIS-G02-002-05', 'KGD-DIS-G02-002-06',
              'MZQ-DIS-G02-001-01', 'MZQ-DIS-G02-001-02', 'MZQ-DIS-G02-001-03', 'MZQ-DIS-G02-001-04', 'MZQ-DIS-G02-001-05', 'MZQ-DIS-G02-001-06',
              'XZH-DIS-G02-001-01', 'XZH-DIS-G02-001-02', 'XZH-DIS-G02-001-03', 'XZH-DIS-G02-002-04', 'XZH-DIS-G02-002-05', 'XZH-DIS-G02-002-06']
    bridge_single = ['S245320707L0010', 'S267320722L0090', 'G204320707L0010']
    sensor_count = [6, 6, 6]
    bridge = []
    for i in range(len(bridge_single)):
        bridge.extend([bridge_single[i]] * sensor_count[i])
    timecycle = [3600*24] * len(sensor)
    point = [5*3600*24] * len(sensor)
    # 共享变量，用于传递数值
    shared_value = [50] * len(sensor)
    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + bridge[i] + "/" + sensor[i]
        topic_new = "cleandata/" + bridge[i] + "/" + sensor[i]
        # print(topic_new)
        mqtt_client_id = "test_" + bridge[i] + "_" + sensor[i]
        string = 'select val from ' + '`' + sensor[i] + '`' + ' order by ts desc' + ' limit ' + str(point[i])
        # print(string)
        # 创建线程
        thread2 = threading.Thread(target=job2, args=(string, sensor[i], i, timecycle[i]))
        thread1 = threading.Thread(target=job, args=(topic, topic_new, mqtt_client_id, i))
        threads.append(thread2)  # 将线程对象添加到列表中
        threads.append(thread1)  # 将线程对象添加到列表中
        thread2.start()  # 启动线程，注意thread1与thread2的顺序
        thread1.start()
        # # 模拟线程运行一段时间后发生异常
        # time.sleep(5)
        # # 故意引发一个异常
        # raise RuntimeError("Simulated thread exception")

    # 等待所有线程创建完成
    for thread2 in threads:  # 循环体放在这不会导致单个线程中的多次循环打印相同值
        thread2.join()
        while True:
            schedule.run_pending()
            time.sleep(1)
    for thread1 in threads:
        thread1.join()










