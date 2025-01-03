import random
import time
import argparse
import traceback
import subprocess
import os
from client import Client
from loguru import logger
from chord_simulation.chord.chord_base import connect_node, hash_func
from chord_simulation.chord.struct_class import Node
import tkinter as tk
import matplotlib.pyplot as plt
import numpy as np
import mplcursors
import matplotlib.font_manager as fm
# 指定中文字体路径，替换为你的字体文件路径
font_path = 'C:/Windows/Fonts/simhei.ttf'  # Windows系统的路径示例
# 加载字体
prop1 = fm.FontProperties(fname=font_path,size=16)
prop2 = fm.FontProperties(fname=font_path,size=10)
parser = argparse.ArgumentParser(description='chord simulation.')
parser.add_argument('-t', '--task_type', type=str, default='finger_table',
                    choices=['basic_query', 'finger_table'],
                    help='simulation type:[basic_query|finger_table]')
parser.add_argument('-n', '--num_nodes', type=int, default=3)
parser.add_argument('-k', '--key_nums', type=int, default=50)

global key_nums,num_nodes,existing_node


def open_terminal_and_run_command(table_type, port):
    # 获取当前工作目录
    current_directory = os.getcwd()

    # 在此目录下打开命令提示符并运行指定的命令
    command = [
        'cmd', '/c', 'start', 'cmd', '/k', f'cd /d "{current_directory}" & python server.py -t {table_type} -p {port}'
    ]
    subprocess.Popen(command)


def build_chord_ring_for_basic_query(num_nodes):
    nodes = []  # 用于存储节点的列表
    global existing_node
    # 创建节点并添加到列表中
    for i in range(num_nodes):
        open_terminal_and_run_command('basic_query',50000 + i + 1)
        time.sleep(2)
        node = Node(hash_func(f'localhost:50000{i+1:02d}'), 'localhost', 50000 + i + 1)
        nodes.append(node)
    existing_node = nodes[0]
    # 连接节点
    for i in range(1, num_nodes):
        conn_prev = connect_node(nodes[i])
        conn_prev.join(nodes[0])  # 所有节点加入到第一个节点

    logger.info("build chord ring...")
    time.sleep(5)  # 等待一段时间以确保所有节点都已加入


def build_chord_ring_for_finger_table(num_nodes):
    nodes = []  # 用于存储节点的列表
    global existing_node
    # 创建节点并添加到列表中
    for i in range(num_nodes):
        open_terminal_and_run_command('finger_table', 50000 + i + 1)
        time.sleep(2)
        node = Node(hash_func(f'localhost:50000{i + 1:02d}'), 'localhost', 50000 + i + 1)
        nodes.append(node)
    existing_node = nodes[0]
    # 连接节点
    for i in range(1, num_nodes):
        conn_prev = connect_node(nodes[i])
        conn_prev.join(nodes[0])  # 所有节点加入到第一个节点

    logger.info("build chord ring...")
    time.sleep(5)  # 等待一段时间以确保所有节点都已加入


def init_data_content(client):
    logger.info("init data content...")
    global key_nums
    for i in range(key_nums):
        client.put(f"key-{i}", f"value-{i}")
        time.sleep(0.5)


def kv_output(node):
    conn_current = connect_node(node)
    output_data = {}

    # 获取节点 ID
    output_data['node_id'] = conn_current.get_id()

    # 获取前驱、后继和本地数据
    predecessor_kv_store = conn_current.get_all_data("predecessor")
    successor_kv_store = conn_current.get_all_data("successor")
    kv_store = conn_current.get_all_data("self")

    # 组织输出数据
    output_data['predecessor'] = {
        key: f'hash({key}) = {hash_func(key)}: {value}'
        for key, value in predecessor_kv_store.items()
    }

    output_data['local'] = {
        key: f'hash({key}) = {hash_func(key)}: {value}'
        for key, value in kv_store.items()
    }

    output_data['successor'] = {
        key: f'hash({key}) = {hash_func(key)}: {value}'
        for key, value in successor_kv_store.items()
    }

    return output_data


def cmd_interaction(client: Client):
    print("请输入要执行的操作")
    cmd = input()
    global key_nums
    global existing_node
    if cmd == "check":
        start_time = time.time()  # 记录开始时间
        expected_kv_store = {f"key-{i}": f"value-{i}" for i in range(key_nums)}  # 创建预期的键值对

        # 用于记录验证结果
        validation_results = {}

        for i in range(key_nums):
            key = f"key-{i}"
            expected_value = expected_kv_store[key]  # 获取预期的值
            query_result = client.get(key)  # 获取实际的值
            # 解析查询结果
            status, returned_key, actual_value, node_id = query_result
            # 验证返回的值是否与预期相符
            if actual_value == expected_value:
                validation_results[key] = "OK"
            else:
                validation_results[key] = f"Error: expected {expected_value}, got {actual_value}"

        end_time = time.time()  # 记录结束时间

        # 计算查询时间
        query_time = end_time - start_time
        print(f'> query time: {query_time:.6f} seconds')  # 打印查询时间，保留6位小数

        # 打印验证结果
        for key, result in validation_results.items():
            print(f"{key}: {result}")
        return

    if cmd == "get_all_data":
        kv_output(existing_node)
        conn_current = connect_node(existing_node)
        start_id = conn_current.get_id()
        next_node = conn_current.get_successor()
        while connect_node(next_node).get_id() != start_id:
            kv_output(next_node)
            next_node = connect_node(next_node).get_successor()
        return

    # 处理添加节点命令
    if cmd.startswith("add_node"):
        params = cmd.split(' ')
        if len(params) != 4:
            print("> Usage: add_node <node_id> <address> <port>")
            return

        node_id, address, port = params[1], params[2], params[3]
        try:
            node_id = hash_func(node_id)  # 确保节点ID为整数
            port = int(port)  # 确保端口为整数
            open_terminal_and_run_command('finger_table', port)
            time.sleep(2)
            conn_prev = connect_node(Node(node_id, address, port))
            conn_prev.join(existing_node)
            time.sleep(5)

        except ValueError:
            print("> Node ID and port must be integers.")
        return

    if cmd.startswith("leave_node"):
        params = cmd.split(' ')
        if len(params) != 4:
            print("> Usage: leave_node <node_id> <address> <port>")
            return

        node_id, address, port = params[1], params[2], params[3]
        try:
            node_id = hash_func(node_id)  # 确保节点ID为整数
            port = int(port)  # 确保端口为整数

            # 创建当前节点的连接
            current_node = Node(node_id, address, port)
            conn_current = connect_node(current_node)

            conn_current.leave_network()

        except ValueError:
            print("> Node ID and port must be integers.")
        return

    params = cmd.split(' ')

    if len(params[0]) == 0:
        return

    if params[0] not in ['put', 'get']:
        print("> only support two operation: put/get")
        return

    if params[0] == 'put' and len(params) == 3:
        key, value = str(params[1]), str(params[2])
        status, node_id = client.put(key, value)
        h = hash_func(key)
        print(f'> hash func({key}) = {h}, put status is {status}, this value will be stored in server-{node_id}')
    elif params[0] == 'get' and len(params) == 2:
        key = str(params[1])
        start_time = time.time()  # 记录开始时间
        status, key, value, node_id = client.get(key)
        end_time = time.time()  # 记录结束时间

        # 计算查询时间
        query_time = end_time - start_time

        h = hash_func(key)
        print(f'> hash func({key}) == {h}, find key in server-{node_id}, get status is {status}.')
        print(f'> get result: key: {key}, value: {value}')
        print(f'> query time: {query_time:.6f} seconds')  # 打印查询时间，保留6位小数
    else:
        print(f'> no support operation format')


def wrap_text(text, width):
    """限制文本宽度并插入换行符，同时保留已有换行符，并在右侧补全空格。每个键值对一行。"""
    # 根据已有换行符分段处理文本
    paragraphs = text.split('\n')
    wrapped_lines = []

    for paragraph in paragraphs:
        # 将每个段落按行分割为键值对
        lines = paragraph.splitlines()

        for line in lines:
            # 分割键值对
            key_value_pairs = line.split(', ')
            for pair in key_value_pairs:
                # 如果当前行加下一个键值对会超过限定宽度，则换行
                if len(pair) > width:
                    # 如果键值对太长，强制换行
                    wrapped_lines.append(pair[:width])
                    wrapped_lines.append(pair[width:].ljust(width))
                else:
                    wrapped_lines.append(pair.ljust(width))

    return "\n".join(wrapped_lines)


def draw_chord_circle_with_interactive_nodes(nodes, max_width=60):
    fig, ax = plt.subplots(figsize=(6,6), subplot_kw={'projection': 'polar'})

    # 计算角度，调整为顺时针并设置正上方为 0 度
    angles = [-2 * np.pi * (node['node_id'] / 65536) + np.pi / 2 for node in nodes]
    node_count = len(nodes)

    scatter_points = []
    for i in range(node_count):
        scatter = ax.scatter(angles[i], 1, s=400, alpha=0.6, label=f'Node {nodes[i]["node_id"]}')
        scatter_points.append(scatter)
        ax.text(angles[i], 1.05, str(nodes[i]['node_id']), ha='center', va='center', fontsize=12)

    # 绘制线段
    for i in range(node_count):
        next_index = (i + 1) % node_count
        ax.plot([angles[i], angles[next_index]], [1, 1], linestyle='--', color='gray')

    # 绘制最外侧的圆环
    theta = np.linspace(0, 2 * np.pi, 100)
    ax.plot(theta, np.ones_like(theta), color='blue', linewidth=2)
    ax.set_title('Chord环可视化(点击节点查看数据)', fontsize=32, fontproperties=prop1)

    cursor = mplcursors.cursor(scatter_points)

    @cursor.connect("add")
    def on_add(sel):
        index = 0
        id0 = int((sel.target[0]-np.pi/2) * 65536 / (-2 * np.pi))
        for i in range(len(nodes)):
            if nodes[i]['node_id'] == id0:
                index = i
                break
        if 0 <= index < len(nodes):
            node_data = nodes[index]
            info_text = (f"Node ID: {node_data['node_id']}\n"  
                         f"Local Data: {node_data.get('local', {})}\n"  
                         f"Predecessor: {node_data.get('predecessor', {})}\n"  
                         f"Successor: {node_data.get('successor', {})}")
            wrapped_text = wrap_text(info_text, max_width)
            sel.annotation.set_fontproperties(prop2)  # 设置字体
            sel.annotation.set_horizontalalignment('left')
            sel.annotation.set_verticalalignment('bottom')
            sel.annotation.set_text(wrapped_text)
            sel.annotation.xy = (angles[index], 1.05)
            sel.annotation.set_bbox(dict(facecolor='yellow', alpha=0.7, edgecolor='black', boxstyle='round,pad=0.5'))


    @cursor.connect("remove")
    def on_remove(sel):
        sel.annotation.set_visible(False)

    plt.axis('off')
    plt.show()


def window_interaction(client: Client):
    global existing_node

    def search():
        key = search_info1.get()
        h = hash_func(key)
        status, key, value, node_id = client.get(key)
        output.delete(1.0, tk.END)
        output.insert(tk.END, f'> hash func({key}) == {h}, find key in server-{node_id},value == {value} ,get status is {status}.')

    def put():
        key = put_info1.get()
        value = put_info2.get()
        status, node_id = client.put(key, value)
        h = hash_func(key)
        output.delete(1.0, tk.END)
        output.insert(tk.END,f'> hash func({key}) = {h}, put status is {status}, this value will be stored in server-{node_id}')

    def add():
        address = add_info1.get()
        port = add_info2.get()
        try:
            node_id = address + ":" + port
            node_id = hash_func(node_id)  # 确保节点ID为整数
            port = int(port)  # 确保端口为整数
            open_terminal_and_run_command('finger_table', port)
            time.sleep(2)
            conn_prev = connect_node(Node(node_id, address, port))
            conn_prev.join(existing_node)
            time.sleep(5)
            output.delete(1.0, tk.END)
            output.insert(tk.END, "加入节点成功")

        except ValueError:
            output.delete(1.0, tk.END)
            output.insert(tk.END, "> port must be integers.")

    def leave():
        address = add_info1.get()
        port = add_info2.get()
        try:
            node_id = address + ":" + port
            node_id = hash_func(node_id)  # 确保节点ID为整数
            port = int(port)  # 确保端口为整数

            # 创建当前节点的连接
            current_node = Node(node_id, address, port)
            conn_current = connect_node(current_node)
            conn_current.leave_network()
            output.delete(1.0, tk.END)
            output.insert(tk.END, "删除节点成功")

        except ValueError:
            output.delete(1.0, tk.END)
            output.insert(tk.END, "> port must be integers.")

    def get_all_data():
        all_node = []
        all_node.append(kv_output(existing_node))
        conn_current = connect_node(existing_node)
        start_id = conn_current.get_id()
        next_node = conn_current.get_successor()
        while connect_node(next_node).get_id() != start_id:
            all_node.append(kv_output(next_node))
            next_node = connect_node(next_node).get_successor()
        draw_chord_circle_with_interactive_nodes(all_node)

    # 创建主窗口
    root = tk.Tk()
    root.title("Chord环模拟")

    search_text = tk.Label(root, text="搜索数据(key)", width=20)
    search_text.grid(row=0, column=0)
    search_info1 = tk.Entry(root, width=20)
    search_info1.grid(row=0, column=1, columnspan=2)
    search_button = tk.Button(root, text="开始搜索", command=search, width=10)
    search_button.grid(row=0, column=3, padx=5)
    put_text = tk.Label(root, text="插入数据(key+value)", width=20)
    put_text.grid(row=1, column=0)
    put_info1 = tk.Entry(root, width=10)
    put_info1.grid(row=1, column=1)
    put_info2 = tk.Entry(root, width=10)
    put_info2.grid(row=1, column=2)
    put_button = tk.Button(root, text="开始插入", command=put, width=10)
    put_button.grid(row=1, column=3, padx=5)
    add_text = tk.Label(root, text="添加节点(address+port)", width=20)
    add_text.grid(row=2, column=0)
    add_info1 = tk.Entry(root, width=10)
    add_info1.grid(row=2, column=1)
    add_info2 = tk.Entry(root, width=10)
    add_info2.grid(row=2, column=2)
    add_button = tk.Button(root, text="开始添加", command=add, width=10)
    add_button.grid(row=2, column=3, padx=5)
    leave_text = tk.Label(root, text="删除节点(address+port)", width=20)
    leave_text.grid(row=3, column=0)
    leave_info1 = tk.Entry(root, width=10)
    leave_info1.grid(row=3, column=1)
    leave_info2 = tk.Entry(root, width=10)
    leave_info2.grid(row=3, column=2)
    leave_button = tk.Button(root, text="开始删除", command=leave, width=10)
    leave_button.grid(row=3, column=3, padx=5)
    output = tk.Text(root, width=50,height=3)
    output.grid(row=4, rowspan=2,column=0, columnspan=4, pady=5)
    plt_show = tk.Button(root,text="作图", width=50,command=get_all_data)
    plt_show.grid(row=7, column=0, columnspan=4, pady=5)
    # 运行主循环
    root.mainloop()

def test():
    def search():
        output.delete(0,tk.END)
        output.insert(tk.END,"查找数据成功")

    def put():
        output.delete(0, tk.END)
        output.insert(tk.END,"添加数据成功")
    # 创建主窗口
    root = tk.Tk()
    root.title("Chord环模拟")

    search_text = tk.Label(root,text="搜索数据",width=10)
    search_text.grid(row=0,column=0)
    search_info1 = tk.Entry(root,width=20)
    search_info1.grid(row=0, column=1,columnspan=2)
    search_button = tk.Button(root, text="开始搜索", command=search,width=10)
    search_button.grid(row=0, column=3, padx = 5)
    put_text = tk.Label(root, text="插入数据", width=10)
    put_text.grid(row=1, column=0)
    put_info1 = tk.Entry(root, width=10)
    put_info1.grid(row=1, column=1)
    put_info2 = tk.Entry(root, width=10)
    put_info2.grid(row=1, column=2)
    put_button = tk.Button(root, text="开始插入", command=put, width=10)
    put_button.grid(row=1, column=3, padx=5)
    add_text = tk.Label(root, text="添加节点", width=10)
    add_text.grid(row=2, column=0)
    add_info1 = tk.Entry(root, width=10)
    add_info1.grid(row=2, column=1)
    add_info2 = tk.Entry(root, width=10)
    add_info2.grid(row=2, column=2)
    add_button = tk.Button(root, text="开始添加", command=put, width=10)
    add_button.grid(row=2, column=3, padx=5)
    leave_text = tk.Label(root, text="删除节点", width=10)
    leave_text.grid(row=3, column=0)
    leave_info1 = tk.Entry(root, width=10)
    leave_info1.grid(row=3, column=1)
    leave_info2 = tk.Entry(root, width=10)
    leave_info2.grid(row=3, column=2)
    leave_button = tk.Button(root, text="开始删除", command=put, width=10)
    leave_button.grid(row=3, column=3, padx=5)
    output = tk.Entry(root, width=40)
    output.grid(row=4, column=0, columnspan=4, pady=5)

    # 运行主循环
    root.mainloop()

def main():
    args = parser.parse_args()
    global key_nums, num_nodes
    key_nums = args.key_nums
    num_nodes = args.num_nodes
    if args.task_type == 'basic_query':
        build_chord_ring_for_basic_query(num_nodes)
    elif args.task_type == 'finger_table':
        build_chord_ring_for_finger_table(num_nodes)

    client = Client("localhost", 50001)
    init_data_content(client)
    window_interaction(client)


if __name__ == '__main__':
    main()

"""
python simulation.py -t finger_table
check
add_node localhost:50005 localhost 50005
leave_node localhost:50005 localhost 50005
"""