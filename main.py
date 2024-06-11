import subprocess

# 启动第一个Python程序
process1 = subprocess.Popen(['python', 'main_clean.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 启动第二个Python程序
process2 = subprocess.Popen(['python', 'main_clean_RSG.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 启动第三个Python程序
process3 = subprocess.Popen(['python', 'main_clean_crk.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 启动第三个Python程序
process4 = subprocess.Popen(['python', 'main_clean_Other.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

try:
    # 等待键盘中断
    process1.wait()
    process2.wait()
    process3.wait()
    process4.wait()
except KeyboardInterrupt:
    # 在捕获到键盘中断时，关闭子进程
    process1.terminate()
    process2.terminate()
    process3.terminate()
    process4.terminate()
    print("所有子进程已终止。")


