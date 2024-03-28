import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import random
import time
'''
main_clean的演示版，无自动重连
增加双线程运行
'''
value = 2
# MQTT 服务器信息
broker_address = "221.226.48.78"
port = 1885
topic = "data/G204320707L0160/XSH-DIS-G02-002-04"  # 替换成你订阅的主题
topic_new = "testdata/G204320707L0160/XSH-DIS-G02-002-04"
mqtt_username = 'jsti_jkjc'
mqtt_password = 'Bridge321'


# 创建一个空的 NumPy 数组用于存储数据
data_array = np.empty((0, 20), dtype=np.float64)  # 假设每秒接收20个数据


# 订阅原始数据的线程
def subscribe_original_data():
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        payload = message.payload
        # 解析二进制数据
        data = struct.unpack(">HBBBBB" + "f" * int(((len(payload))-7)/4), payload)  # 解析后的二进制数据
        year = data[0]
        month = data[1]
        day = data[2]
        hour = data[3]
        minute = data[4]
        second = data[5]
        data_num = np.array(data[6:11], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
        processed_data = process_data(data_num)
        polluted_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second, *processed_data)
        mqtt_publish(topic_new, polluted_data)
        print('polluted_data:', processed_data)

    def process_data(data):
        idx = random.randint(1,4)
        data[idx] = 99.9999999
        return data

    def mqtt_publish(mqtt_topic, record_data):
        try:
            client.publish(mqtt_topic, record_data, qos=1)
            # print(f"Published to {mqtt_topic}")
        except Exception as e:
            print(f"MQTT publish failed: {e}")

    # 创建 MQTT 客户端实例
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    # 设置消息到来时的回调函数
    client.on_message = on_message
    # 连接 MQTT 服务器
    client.connect(broker_address, port)
    # 订阅主题
    client.subscribe(topic)
    # 循环监听消息
    client.loop_forever()


# 订阅处理后数据的线程
def subscribe_processed_data():
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        payload = message.payload
        # 解析二进制数据
        data = struct.unpack(">HBBBBB" + "f" * int(((len(payload))-7)/4), payload)  # 解析后的二进制数据
        year = data[0]
        month = data[1]
        day = data[2]
        hour = data[3]
        minute = data[4]
        second = data[5]
        data_num = np.array(data[6:11], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
        processed_data = process_data(data_num, value)
        print('cleaned_data:', processed_data)

    def process_data(data, value):
        # 统计超过参考值的索引
        exceed_indices = np.where(data > value)[0]
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

    # 创建 MQTT 客户端实例
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    # 设置消息到来时的回调函数
    client.on_message = on_message
    # 连接 MQTT 服务器
    client.connect(broker_address, port)
    # 订阅主题
    client.subscribe(topic_new)
    # 循环监听消息
    client.loop_forever()


# 创建并启动线程
t1 = threading.Thread(target=subscribe_original_data)
t2 = threading.Thread(target=subscribe_processed_data)
t1.start()
t2.start()
t1.join()
t2.join()
