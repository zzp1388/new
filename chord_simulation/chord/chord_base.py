import hashlib
import thriftpy2
import os
import threading
import traceback
from thriftpy2.rpc import make_client
from .struct_class import KeyValueResult, Node, M
from loguru import logger

# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 构造 Thrift IDL 文件的路径
thrift_path = os.path.join(current_dir, '../idl/chord.thrift')
# 加载 Thrift 文件，生成对应的 Python 模块
chord_thrift = thriftpy2.load(thrift_path, module_name='chord_thrift')


class BaseChordNode:
    """
    Chord 节点的基本接口
    """

    def __init__(self):
        self.logger = logger  # 日志记录器
        self._interval = 1  # 定时器间隔
        self.__timer = threading.Timer(self._interval, self.run_periodically)  # 创建定时器
        self.__timer.start()  # 启动定时器
        self.predecessor = None  # 前驱节点初始化为 None
        self.successor = None  # 后继节点初始化为 None
        self.node_id = 0  # id初始化为0

    def lookup(self, key: str) -> KeyValueResult:
        """查找给定键的值，未实现的抽象方法"""
        raise NotImplementedError

    def _lookup_local(self, key: str) -> KeyValueResult:
        """本地查找给定键的值，未实现的抽象方法"""
        raise NotImplementedError

    def find_successor(self, key_id: int) -> Node:
        """查找给定键 ID 的后继节点，未实现的抽象方法"""
        raise NotImplementedError

    def _closet_preceding_node(self, key_id: int) -> Node:
        """查找给定键 ID 的最近前驱节点，未实现的抽象方法"""
        raise NotImplementedError

    def put(self, key: str, value: str) -> KeyValueResult:
        """存储键值对，未实现的抽象方法"""
        raise NotImplementedError

    def do_put(self, key: str, value: str, place: str) -> KeyValueResult:
        """存储键值对，未实现的抽象方法"""
        raise NotImplementedError

    def join(self, node: Node):
        """加入给定节点，未实现的抽象方法"""
        raise NotImplementedError

    def _stabilize(self):
        """稳定性检查，未实现的抽象方法"""
        raise NotImplementedError

    def notify(self, node: Node):
        """通知节点，未实现的抽象方法"""
        raise NotImplementedError

    def _fix_fingers(self):
        """修复指针，未实现的抽象方法"""
        raise NotImplementedError

    def _check_predecessor(self):
        """检查前驱节点的有效性，未实现的抽象方法"""
        raise NotImplementedError

    def get_predecessor(self) -> Node:
        """获取当前节点的前驱节点"""
        return self.predecessor

    def get_successor(self) -> Node:
        """获取当前节点的后继节点"""
        return self.successor

    def get_id(self) -> int:
        """获取当前节点的后继节点"""
        return self.node_id

    def _log_self(self):
        """记录当前节点的信息，未实现的抽象方法"""
        raise NotImplementedError

    def is_successor_alive(self):
        raise NotImplementedError

    def run_periodically(self):
        """定期运行的任务"""
        try:
            self._stabilize()  # 稳定性检查
            self._fix_fingers()  # 修复指针
            self._check_predecessor()  # 检查前驱节点
            self.update_data()  # 更新数据
            # self.update_successor_kv_store() # 维护successor_kv_store
            # self.update_predecessor_kv_store() # 维护predecessor_kv_store
            self._log_self()  # 记录当前节点信息

        except Exception as e:
            self.logger.warning(e)  # 记录警告信息
            self.logger.warning(traceback.format_exc())  # 记录异常堆栈信息

        # 重新设置定时器
        self.__timer = threading.Timer(self._interval, self.run_periodically)
        self.__timer.start()  # 启动下一个定时任务

    def migrate_data(self):
        raise NotImplementedError

    def check_and_clean_data(self):
        raise NotImplementedError

    def get_all_data(self, place: str):
        raise NotImplementedError

    def is_key_for_node(self, key: str):
        raise NotImplementedError

    def update_successor_kv_store(self):
        raise NotImplementedError

    def update_predecessor_kv_store(self):
        raise NotImplementedError

    def leave_network(self):
        raise NotImplementedError

    def update_predecessor(self, predecessor):
        raise NotImplementedError

    def update_successor(self, successor):
        raise NotImplementedError

    def update_data(self):
        raise NotImplementedError

def hash_func(intput_str) -> int:
    """
    使用 SHA-1 哈希函数
    """
    sha1 = hashlib.sha1()  # 创建 SHA-1 哈希对象
    sha1.update(str(intput_str).encode('utf-8'))  # 更新哈希对象
    hash_hex = sha1.hexdigest()  # 获取哈希值的十六进制表示
    hash_int = int(hash_hex, 16)  # 将十六进制转换为整数
    hash_int = hash_int % (2 ** M)  # 对 2^M 取模
    return hash_int  # 返回哈希值


def connect_address(address, port):
    """
    尝试连接指定的地址和端口，如果在线则返回节点对象，否则返回 None
    """
    try:
        node = make_client(chord_thrift.ChordNode, address, port)  # 创建 Thrift 客户端
        return node  # 返回节点对象
    except Exception as e:
        logger.warning(e)  # 记录警告信息
        logger.warning(traceback.format_exc())  # 记录异常堆栈信息
        return None  # 返回 None


def connect_node(node: Node):
    """
    尝试连接节点，如果节点在线则返回节点对象，否则返回 None
    """
    return connect_address(node.address, node.port)  # 通过地址和端口连接节点


def is_between(node: Node, node1: Node, node2: Node):
    """
    判断节点是否位于顺时针弧（node1 -> node2）上，包括 node2 但不包括 node1。
    这就像判断节点是否在 (node1, node2] 区间内。
    """
    start_node_id, end_node_id = node1.node_id, node2.node_id  # 获取节点 ID
    if start_node_id < end_node_id:
        return start_node_id < node.node_id <= end_node_id  # 顺时针情况
    elif start_node_id == end_node_id:
        return True  # 相等的情况
    else:
        return node.node_id > start_node_id or node.node_id <= end_node_id  # 逆时针情况