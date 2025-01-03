from __future__ import annotations

import thriftpy2
import os

# 定义一个常量 M，表示 Chord 协议中的节点数
M = 16

# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 拼接获取 Chord 协议的 Thrift 文件路径
thrift_path = os.path.join(current_dir, '../idl/chord.thrift')
# 加载 Thrift 文件，生成相应的 Python 类
chord_thrift = thriftpy2.load(thrift_path, module_name='chord_thrift')


# 定义 KVStatus 类，继承自 Thrift 生成的 KVStatus 类
class KVStatus(chord_thrift.KVStatus):
    VALID = chord_thrift.KVStatus.VALID  # 有效状态
    NOT_FOUND = chord_thrift.KVStatus.NOT_FOUND  # 未找到状态


# 定义 KeyValueResult 类，继承自 Thrift 生成的 KeyValueResult 类
class KeyValueResult(chord_thrift.KeyValueResult):
    def __init__(self, key: str, value: str, node_id: int, status: KVStatus = KVStatus.VALID):
        # 初始化 KeyValueResult，设置键、值、节点 ID 和状态
        super().__init__(key, value, node_id, status)


# 定义 Node 类，继承自 Thrift 生成的 Node 类
class Node(chord_thrift.Node):
    def __init__(self, node_id: int, address: str, port: int, valid: bool = True):
        # 初始化 Node，设置节点 ID、地址、端口和有效性
        super().__init__(node_id, address, port, valid)