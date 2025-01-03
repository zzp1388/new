from ..chord.chord_base import BaseChordNode
from ..chord.chord_base import connect_node, hash_func, is_between
from ..chord.struct_class import KeyValueResult, Node, KVStatus
import threading

class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()

        self.node_id = hash_func(f'{address}:{port}')
        self.kv_store = dict()
        self.predecessor_kv_store = dict()  # 存储前驱节点的键值对
        self.successor_kv_store = dict()  # 存储后继节点的键值对

        self.self_node = Node(self.node_id, address, port)
        self.successor = self.self_node
        self.predecessor = Node(self.node_id, address, port, valid=False)
        self.stability_test_paused = False  # 跟踪稳定性测试的状态
        self.logger.info(f'node {self.node_id} listening at {address}:{port}')

    def _log_self(self):
        msg = 'now content: '
        msg += '\nlocal:'
        for k, v in self.kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        msg += '\npredecessor:'
        for k, v in self.predecessor_kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        msg += '\nsuccessor:'
        for k, v in self.successor_kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)

        pre_node_id = self.predecessor.node_id if self.predecessor.valid else "null"
        self.logger.debug(f"{pre_node_id} - {self.node_id} - {self.successor.node_id}")

    def lookup(self, key: str) -> KeyValueResult:
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self._lookup_local(key)
        else:
            next_node = self._closet_preceding_node(h)
            conn_next_node = connect_node(next_node)
            return conn_next_node.lookup(key)

    def _lookup_local(self, key: str) -> KeyValueResult:
        result = self.kv_store.get(key, None)
        status = KVStatus.VALID if result is not None else KVStatus.NOT_FOUND
        return KeyValueResult(key, result, self.node_id, status)

    def find_successor(self, key_id: int) -> Node:
        key_id_node = Node(key_id, "", 0)
        if is_between(key_id_node, self.self_node, self.successor):
            return self.successor
        else:
            next_node = self._closet_preceding_node(key_id)
            conn_next_node = connect_node(next_node)
            return conn_next_node.find_successor(key_id)

    def _closet_preceding_node(self, key_id: int) -> Node:
        return self.successor

    def put(self, key: str, value: str) -> KeyValueResult:
        h = hash_func(key)  # 计算哈希值
        tmp_key_node = Node(h, "", 0)

        # 判断 key 是否在当前节点（self_node）和前驱节点之间
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            # 在当前节点执行插入
            result = self.do_put(key, value, "self")

            # 尝试将副本插入前驱节点
            if self.predecessor and self.predecessor.valid:
                try:
                    predecessor_client = connect_node(self.predecessor)
                    predecessor_client.do_put(key, value, "successor")  # 直接调用 do_put，假设该方法也可处理
                    print(f"Stored ({key}, {value}) in predecessor {self.predecessor.node_id}.")
                except Exception as e:
                    print(f"Failed to store in predecessor {self.predecessor.node_id}: {e}")

            # 尝试将副本插入后继节点
            if self.successor and self.successor.valid:
                try:
                    successor_client = connect_node(self.successor)
                    successor_client.do_put(key, value, "predecessor")  # 直接调用 do_put
                    print(f"Stored ({key}, {value}) in successor {self.successor.node_id}.")
                except Exception as e:
                    print(f"Failed to store in successor {self.successor.node_id}: {e}")

            return result

        # 如果不在该范围内，寻找合适的下一个节点
        next_node = self._closet_preceding_node(h)
        conn_next_node = connect_node(next_node)

        # 将请求传递给下一个节点
        return conn_next_node.put(key, value)

    def do_put(self, key: str, value: str, place: str) -> KeyValueResult:
        # 存储当前节点的数据
        if place == "self":
            self.kv_store[key] = value
        elif place == "predecessor":
            self.predecessor_kv_store[key] = value
        else :
            self.successor_kv_store[key] = value

        return KeyValueResult(key, value, self.node_id)

    def join(self, node: Node):
        conn_node = connect_node(node)
        self.successor = conn_node.find_successor(self.node_id)

    def notify(self, node: Node):
        if not self.predecessor.valid or is_between(node, self.predecessor, self.self_node):
            self.predecessor = node

    def _stabilize(self):
        if not self.stability_test_paused:
            try:
                conn_successor = connect_node(self.successor)
                x = conn_successor.get_predecessor()

                # 确保 x 是有效节点
                if x and is_between(x, self.self_node, self.successor):
                    print(f"Updating successor from {self.successor.node_id} to {x.node_id}.")
                    self.successor = x

                # 通知后继节点当前节点
                conn_successor.notify(self.self_node)

            except Exception as e:
                print(f"An error occurred during stabilization: {e}")

    def pause_stability_tests(self):
        self.stability_test_paused = True

    def resume_stability_tests(self):
        self.stability_test_paused = False


    def _fix_fingers(self):
        pass

    def _check_predecessor(self):
        pass

    def migrate_data(self):
        # Connect to predecessor and successor nodes
        try:
            predecessor_client = connect_node(self.predecessor)
            successor_client = connect_node(self.successor)
        except Exception as e:
            print(f"Error connecting to nodes: {e}")
            return  # 连接失败，停止操作

        # 将后继节点的 kv_pairs 复制到本节点
        try:
            successor_client = connect_node(self.successor)
            kv_pairs_1 = successor_client.get_all_data("self")
            for key, value in kv_pairs_1.items():
                print(key,value,'\n')
                self.kv_store[key] = value

            # 更新本节点与后继节点的数据
            self.check_and_clean_data()
            successor_client = connect_node(self.successor)
            successor_client.check_and_clean_data()
        except Exception as e:
            print(f"Error during data migration from successor: {e}")
            return  # 在数据迁移出错时停止操作

        try:
            # 更新前驱节点的 successor_kv_store
            predecessor_client = connect_node(self.predecessor)
            predecessor_client.update_successor_kv_store()

            # 更新后继节点的 predecessor_kv_store
            successor_client = connect_node(self.successor)
            successor_client.update_predecessor_kv_store()

            # 更新后继节点的后继节点的 predecessor_kv_store
            s_successor_client = connect_node(successor_client.get_successor())
            s_successor_client.update_predecessor_kv_store()

        except Exception as e:
            print(f"Error updating predecessor and successor KV stores: {e}")
            return  # 更新失败停止操作

        # 更新本节点的 predecessor_kv_store 和 successor_kv_store
        try:
            kv_pairs_3 = successor_client.get_all_data("self")
            for key, value in kv_pairs_3.items():
                self.successor_kv_store[key] = value

            kv_pairs_4 = predecessor_client.get_all_data("self")
            for key, value in kv_pairs_4.items():
                self.predecessor_kv_store[key] = value

        except Exception as e:
            print(f"Error retrieving KV pairs from predecessor or successor: {e}")
            return  # 发生错误时停止操作

        # 数据迁移完成
        print("Data migration completed successfully.")

    def check_and_clean_data(self):
        """对当前节点的所有数据进行检查，删除不符合条件的数据"""
        keys_to_delete = []

        for key in list(self.kv_store.keys()):  # 使用 list() 防止在遍历时修改字典
            if not self.is_key_for_node(key):  # 根据需要检查数据
                keys_to_delete.append(key)

        # 删除不符合条件的数据
        for key in keys_to_delete:
            del self.kv_store[key]

        self.logger.info(f"Data cleaned for node {self.node_id}. Remaining keys: {list(self.kv_store.keys())}")

    def get_all_data(self, place: str):
        if place == "self":
            return self.kv_store
        elif place == "predecessor":
            return self.predecessor_kv_store
        else:
            return self.successor_kv_store

    def is_key_for_node(self, key: str):
        """判断一个键是否应当属于某个节点，由节点ID决定键是否属于该节点"""
        h = hash_func(key)  # 计算哈希值
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return True
        return False

    def update_successor_kv_store(self):
        successor_client = connect_node(self.successor)
        kv_pairs = successor_client.get_all_data("self")
        self.successor_kv_store.clear()
        # 更新successor_kv_store
        for key, value in kv_pairs.items():
            self.successor_kv_store[key] = value
        print(f"Updated successor kv_store with data: {kv_pairs}")

    def update_predecessor_kv_store(self):
        predecessor_client = connect_node(self.predecessor)
        kv_pairs = predecessor_client.get_all_data("self")
        self.predecessor_kv_store.clear()
        # 更新predecessor_kv_store
        for key, value in kv_pairs.items():
            self.predecessor_kv_store[key] = value
        print(f"Updated predecessor kv_store with data: {kv_pairs}")

    def leave_network(self):
        successor_client = connect_node(self.successor)
        successor_client.pause_stability_tests()
        predecessor_client = connect_node(self.predecessor)
        predecessor_client.pause_stability_tests()
        self.pause_stability_tests()
        successor_client = connect_node(self.successor)
        successor_client.update_predecessor(self.predecessor)
        predecessor_client = connect_node(self.predecessor)
        predecessor_client.update_successor(self.successor)
        successor_client = connect_node(self.successor)
        successor_client.resume_stability_tests()
        predecessor_client = connect_node(self.predecessor)
        predecessor_client.resume_stability_tests()
        for key, value in self.kv_store.items():
            successor_client = connect_node(self.successor)
            successor_client.put(key, value)
        self.predecessor = self.self_node
        self.successor = self.self_node
        self.kv_store.clear()

    def update_predecessor(self, predecessor):
        self.predecessor = predecessor  # 更新前驱

    def update_successor(self, successor):
        self.successor = successor  # 更新后继