import paho.mqtt.client as mqtt
import struct
import numpy as np
import threading
import time
'''
实时在线滤除异常数据正式版
订阅-转换-处理-转换-发布
添加自动重连
'''
value = 50
# MQTT 服务器信息
broker_address = "221.226.48.78"
port = 1885
topic = "data/G204320707L0160/XSH-DIS-G02-002-04"  # 替换成你订阅的主题
topic_new = "cleandata/G204320707L0160/XSH-DIS-G02-002-04"
mqtt_username = 'jsti_jkjc'
mqtt_password = 'Bridge321'
mqtt_client_id = "clean_G204320707L0160_DIS"  # 指定您的 MQTT 客户端标识符

# 创建一个空的 NumPy 数组用于存储数据
data_array = np.empty((0, 20), dtype=np.float64)  # 假设每秒接收20个数据

# 锁
mqtt_lock = threading.Lock()


# MQTT 消息到来时的回调函数
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
    cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second, *processed_data)
    mqtt_publish(topic_new, cleaned_data)


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


def mqtt_publish(mqtt_topic, record_data):
    with mqtt_lock:
        try:
            client.publish(mqtt_topic, record_data, qos=1)
            print(f"Published to {mqtt_topic}")
        except Exception as e:
            print(f"MQTT publish failed: {e}")


# 创建 MQTT 客户端实例
client = mqtt.Client(client_id=mqtt_client_id, clean_session=True)
client.username_pw_set(username=mqtt_username, password=mqtt_password)
# 设置消息到来时的回调函数
client.on_message = on_message
# 设置断开连接时的回调函数
client.on_reconnect = on_reconnect
# 连接 MQTT 服务器
client.connect(broker_address, port)
# 订阅主题
client.subscribe(topic)
# 循环监听消息
client.loop_forever()