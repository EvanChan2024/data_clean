import paho.mqtt.client as mqtt
import threading
import time
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
log_directory = r'D:\Project\01\03.onlineclean\log'
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
                logger.info(f"{thread_current}: Connected to MQTT broker")
            else:
                thread_current = check_current_thread()
                logger.info(f"{thread_current}: Connection failed")
            payload = message.payload
            mqtt_publish(destination_client, topic1, payload)
        except Exception as e:
            logger.error(f"Data subscribing failed: {e}, message: {message}")

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
    source_client.connect(source_broker_address, source_port)
    destination_client.connect(destination_broker_address, destination_port)

    # 循环监听消息
    source_client.loop_start()
    destination_client.loop_forever()


if __name__ == "__main__":
    df = pd.read_excel(r'D:\gzwj\01.重点工作\sensorinfo_part.xlsx', sheet_name='BRIDGE_TEST_SELFCHECK.T_BRIDGE')
    filtered_data = df[~df['SENSOR_SUB_TYPE_NAME'].isin(['竖向位移', '主梁竖向位移', '主梁竖向位移监测', '主梁位移', '应变/温度', '结构应变监测(振弦)', '应变温度', '结构应力', '结构裂缝', 'LVDT裂缝监测', '拉绳位移监测', '梁端纵向位移', '裂缝'])][['FOREIGN_KEY', 'SENSOR_CODE']]
    bridge = filtered_data['FOREIGN_KEY'].to_list()
    sensor = filtered_data['SENSOR_CODE'].to_list()

    threads = []  # 创建一个列表来存储线程对象
    for i in range(len(sensor)):
        topic = "data/" + bridge[i] + "/" + sensor[i]
        mqtt_client_id = "clean_1_" + bridge[i] + "_" + sensor[i]
        mqtt_client_id2 = "clean2_1_" + bridge[i] + "_" + sensor[i]
        # 创建线程
        thread1 = threading.Thread(target=job, args=(topic, mqtt_client_id, mqtt_client_id2, i))
        threads.append(thread1)  # 将线程对象添加到列表中
        thread1.start()

    for thread1 in threads:
        thread1.join()
