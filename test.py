import numpy as np
'''
【实时滤除跳点】核心数据处理模块，使用一维数组测试
可以滤除超限点，并进行数据替换，支持孤立点及连续的两个点
'''


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
    return data, abnormal_indices


# 示例数据和参考值
data = np.array([1.2, 0.3, 1.2, 1.3, 1.2, 0.2, 0.35, 1.45, 0.55])
value = 1

# 处理数据并获取异常索引
processed_data, abnormal_indices = process_data(data, value)
print("处理后的数据：", processed_data)
print("异常索引列表：", abnormal_indices)