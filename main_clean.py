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


# interval_in_seconds = 3600
# schedule.every(interval_in_seconds).seconds.do(read_data)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)


def read_data(statement):
    conn: taos.TaosConnection = taos.connect(host="jkjc1",
                                             user="root",
                                             password="taosdata",
                                             database="db_jkjc",
                                             port=6030)
    result = conn.query(statement)
    result_list = result.fetch_all()  # 将查询结果转换为列表
    thre = threshold_cal(result_list)
    return thre


def threshold_cal(data):
    threshold = 2 * np.percentile(np.abs(data), 99.5)  # 先将data转为numpy数组
    return threshold


def job(topic, topic_new, mqtt_client_id, thre):
    # MQTT 消息到来时的回调函数
    def on_message(client, userdata, message):
        # 接收到消息时的处理逻辑
        payload = message.payload
        unpack(payload)

    def unpack(payload):
        # 解析二进制数据
        data = struct.unpack(">HBBBBB" + "f" * int(((len(payload))-7)/4), payload)  # 解析后的二进制数据
        payload_len = int(((len(payload))-7)/4)
        pack(data, payload_len, thre)

    def pack(data, payload_len, threshold):
        year = data[0]
        month = data[1]
        day = data[2]
        hour = data[3]
        minute = data[4]
        second = data[5]
        data_num = np.array(data[6:payload_len+6], dtype=np.float64)  # 提取所需的数据部分并转换为 NumPy 数组
        processed_data = process_data(data_num, threshold)
        cleaned_data = struct.pack(">HBBBBB" + "f" * len(processed_data), year, month, day, hour, minute, second, *processed_data)
        mqtt_publish(client, topic_new, cleaned_data)
        # print(processed_data)

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
    client = mqtt.Client(client_id=mqtt_client_id, clean_session=True)  # 一个线程一个client_id即可
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


if __name__ == "__main__":
    sensor = ['XSH-DIS-G02-001-01', 'XSH-DIS-G02-001-02']
    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + "G204320707L0160/" + sensor[i]
        topic_new = "cleandata/" + "G204320707L0160/" + sensor[i]
        mqtt_client_id = "test_G204320707L0160_" + sensor[i]
        string = 'select val from ' + '`' + sensor[i] + '`' + ' limit 1800'
        thre = read_data(string)  # 怎么把语句传过去？
        print(string)

        # 创建线程
        thread = threading.Thread(target=job, args=(topic, topic_new, mqtt_client_id, thre))
        threads.append(thread)  # 将线程对象添加到列表中
        thread.start()  # 启动线程

    # 等待所有线程创建完成
    for thread in threads:
        thread.join()


