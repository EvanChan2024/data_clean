import subprocess

# 启动第一个Python程序
process1 = subprocess.Popen(['python', 'main_clean_ND01.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第一个Python程序
process2 = subprocess.Popen(['python', 'main_clean_ND02.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第二个Python程序
process3 = subprocess.Popen(['python', 'main_clean_CRK.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第三个Python程序
process4 = subprocess.Popen(['python', 'main_clean_RSG.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第三个Python程序
process5 = subprocess.Popen(['python', 'main_clean_Other01.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第三个Python程序
process6 = subprocess.Popen(['python', 'main_clean_Other02.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 启动第三个Python程序
process7 = subprocess.Popen(['python', 'main_clean_ND03.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

try:
    # 等待键盘中断
    process1.wait()
    process2.wait()
    process3.wait()
    process4.wait()
    process5.wait()
    process6.wait()
    process7.wait()
except KeyboardInterrupt:
    # 在捕获到键盘中断时，关闭子进程
    process1.terminate()
    process2.terminate()
    process3.terminate()
    process4.terminate()
    process5.terminate()
    process6.terminate()
    process7.terminate()
    print("所有子进程已终止。")


