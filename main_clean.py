import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import time
import taos
import schedule
'''
实时在线滤除异常数据正式版
订阅-转换-处理-转换-发布
添加自动重连
'''

# 锁
mqtt_lock = threading.Lock()


def job2(string, sensorcode, thread_index, cycle):
    def read_data(statement):
        conn: taos.TaosConnection = taos.connect(host="dbmaster",
                                                 user="root",
                                                 password="taosdata",
                                                 database="db_jkjc",
                                                 port=6030)
        result1 = conn.query(statement)
        result_list = result1.fetch_all()  # 将查询结果转换为列表
        # print(result_list)
        thre = threshold_cal(result_list)
        return thre

    def threshold_cal(data):
        q75 = np.percentile(data, 95) + (np.percentile(data, 95) - np.percentile(data, 5)) * 10  # 先将data转为numpy数组
        q25 = np.percentile(data, 5) - (np.percentile(data, 95) - np.percentile(data, 5)) * 10
        threshold = np.max(np.abs(q75), np.abs(q25))
        return threshold

    def job3():
        global shared_value
        result2 = read_data(string)
        shared_value[thread_index] = result2
        # print(sensorcode + '动态阈值:', shared_value)

    # job3()
    # 定时任务
    interval_in_seconds = cycle
    schedule.every(interval_in_seconds).seconds.do(job3)  # 循环体放在这会导致单个线程中的多次循环打印相同值
    # event.set()  # 设置事件


def job(topic1, topic1_new, mqtt_client_id1, thread_index):
    # MQTT 消息到来时的回调函数
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        # topic2 = message.topic
        payload = message.payload
        unpack(payload)
        # print(topic2)

    def unpack(payload):
        # 解析二进制数据
        data = struct.unpack(">HBBBBB" + "f" * int(((len(payload))-7)/4), payload)  # 解析后的二进制数据
        payload_len = int(((len(payload))-7)/4)
        pack(data, payload_len)

    def pack(data, payload_len):
        year = data[0]
        month = data[1]
        day = data[2]
        hour = data[3]
        minute = data[4]
        second = data[5]
        global shared_value
        data_num = np.array(data[6:payload_len+6], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
        processed_data = process_data(data_num, shared_value[thread_index])
        cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second, *processed_data)
        mqtt_publish(client, topic1_new, cleaned_data)
        # print(processed_data)
        print(shared_value)

    def process_data(data, value):
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

    def mqtt_publish(client, mqtt_topic, record_data):
        with mqtt_lock:
            try:
                client.publish(mqtt_topic, record_data, qos=1)
                print(f"Published to {mqtt_topic}")
            except Exception as e:
                print(f"MQTT publish failed: {e}")

    def on_reconnect(client, userdata, rc):
        while rc != 0:
            print(f"Unexpected disconnection. Will try to reconnect... ({rc})")
            try:
                time.sleep(10)  # 设置重连尝试的间隔，例如等待10秒
                client.reconnect()
                print("Reconnection successful")
                rc = 0  # 如果成功重连，将 rc 设为 0 以退出循环
            except Exception as e:
                print(f"Reconnection failed: {e}")

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
    sensor = ['XSH-DIS-G02-001-01', 'XSH-DIS-G02-001-02', 'XSH-DIS-G02-001-03', 'XSH-DIS-G02-002-04', 'XSH-DIS-G02-002-05', 'XSH-DIS-G02-002-06', 'XSH-RSG-G02-001-01', 'XSH-RSG-G02-001-02', 'XSH-RSG-G02-001-03']
    timecycle = [3600*24, 3600*24, 3600*24, 3600*24, 3600*24, 3600*24, 3600*24, 3600*24, 3600*24]
    point = [5*3600*24, 5*3600*24, 5*3600*24, 5*3600*24, 5*3600*24, 5*3600*24, 144, 144, 144]
    # 共享变量，用于传递数值
    shared_value = [50] * len(sensor)
    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + "G204320707L0160/" + sensor[i]
        topic_new = "clean2data/" + "G204320707L0160/" + sensor[i]
        # print(topic_new)
        mqtt_client_id = "test2_G204320707L0160_" + sensor[i]
        string = 'select val from ' + '`' + sensor[i] + '`' + ' order by ts desc' + ' limit ' + str(point[i])
        # print(string)
        # event = threading.Event()
        # 创建线程
        thread2 = threading.Thread(target=job2, args=(string, sensor[i], i, timecycle[i]))
        thread1 = threading.Thread(target=job, args=(topic, topic_new, mqtt_client_id, i))
        threads.append(thread2)  # 将线程对象添加到列表中
        thread2.start()  # 启动线程，注意thread1与thread2的顺序
        threads.append(thread1)  # 将线程对象添加到列表中
        thread1.start()

    # 等待所有线程创建完成
    for thread2 in threads:  # 循环体放在这不会导致单个线程中的多次循环打印相同值
        thread2.join()
        while True:
            schedule.run_pending()
            time.sleep(1)
    for thread1 in threads:
        thread1.join()

