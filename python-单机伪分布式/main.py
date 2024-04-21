from mapper import Mapper
from reducer import Reducer
import pickle
import queue
from collections import defaultdict
import multiprocessing
import sys
import os


def worker_map(map_input: queue, map_output: queue):
    """
    map方法，将输入解析为 (xx, 1) 的形式
    :param map_input: ["a bb", "bb cc"]
    :param map_output: [('a', 1), ('bb', 1), ('bb', 1), ('cc', 1)]
    """
    while not map_input.empty():
        line = map_input.get()
        for result in mapper.map(line):
            map_output.put(result)


def shuffle(map_output: queue, reduce_input: queue):
    """
    shuffle方法，将map的输出shuffle为 [K2, list(V2)] 的形式
    :param map_output: [('a', 1), ('bb', 1), ('bb', 1), ('cc', 1)]
    :param reduce_input: [('a', [1]), ('bb', [1, 1]), ('cc', [1])]
    """
    shuffled_data = defaultdict(list)

    while not map_output.empty():
        key, value = map_output.get()
        shuffled_data[key].append(value)

    for key, values in shuffled_data.items():
        reduce_input.put((key, values))


def worker_reduce(redece_input: queue, reduce_output: queue):
    """
    reduce方法，对shuffle的结果求和输出
    :param redece_input: [('a', [1]), ('bb', [1, 1]), ('cc', [1])]
    :param reduce_output: [('a', 1), ('bb', 2), ('cc', 1)]
    """
    while not redece_input.empty():
        key, value = redece_input.get()
        reduce_output.put(reducer.reduce(key, value))


if __name__ == '__main__':
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    mapper_path = sys.argv[3]
    reducer_path = sys.argv[4]

    mapper: Mapper
    with open(mapper_path, 'rb') as f:
        mapper = pickle.load(f)()
    reducer: Reducer
    with open(reducer_path, 'rb') as f:
        reducer = pickle.load(f)()

    map_input = queue.Queue()
    map_output = queue.Queue()
    reduce_input = queue.Queue()
    reduce_output = queue.Queue()
    num_workers = 1

    # 读取输入文件
    with open(input_path, 'r') as f:
        for line in f:
            map_input.put(line.strip())

    # worker_map(map_input, map_output)
    # shuffle(map_output, reduce_input)
    # worker_reduce(reduce_input, reduce_output)
    # print(list(reduce_output.queue))

    # 创建并启动Mapper进程
    map_processes = [multiprocessing.Process(target=worker_map, args=(map_input, map_output))
                     for i in range(num_workers)]
    for p in map_processes:
        p.start()
    for p in map_processes:
        p.join()

    # shuffle数据
    shuffle(map_output, reduce_input)

    # 创建并启动Reducer进程
    reduce_processes = [multiprocessing.Process(target=worker_reduce, args=(reduce_input, reduce_output))
                        for i in range(num_workers)]
    for p in reduce_processes:
        p.start()
    for p in reduce_processes:
        p.join()

    # 输出结果
    with open(output_path, 'w') as f:
        while not reduce_output.empty():
            key, count = reduce_output.get()
            f.write(f'{key}\t{count}\n')
