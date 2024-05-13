import subprocess

# 启动第一个Python程序
process1 = subprocess.Popen(['python', 'main_clean.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 启动第二个Python程序
process2 = subprocess.Popen(['python', 'main_clean_RSG.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 启动第三个Python程序
process3 = subprocess.Popen(['python', 'main_clean_Other.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

try:
    # 读取并打印第一个子进程的输出
    for line in iter(process1.stdout.readline, b''):
        print(line.decode('utf-8').strip())

    # 读取并打印第二个子进程的输出
    for line in iter(process2.stdout.readline, b''):
        print(line.decode('utf-8').strip())

    # 读取并打印第三个子进程的输出
    for line in iter(process3.stdout.readline, b''):
        print(line.decode('utf-8').strip())


except KeyboardInterrupt:
    # 在捕获到键盘中断时，关闭子进程
    process1.terminate()
    process2.terminate()


