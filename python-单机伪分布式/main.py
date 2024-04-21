from mapper import Mapper
from reducer import Reducer
import multiprocessing
import sys
import os
import queue
import pickle


def worker_map(queue_input: queue, queue_output: queue):
    while not queue_input.empty():
        line = queue_input.get()
        for result in mapper.map(line):
            queue_output.put(result)


def worker_reduce(queue_input: queue, queue_output: queue):
    intermediate = {}
    while not queue_input.empty():
        key, value = queue_input.get()
        if key in intermediate:
            intermediate[key].append(value)
        else:
            intermediate[key] = [value]

    for key, values in intermediate.items():
        queue_output.put(reducer.reduce(key, values))


def main(input_path: str, output_path: str, num_workers: int = 2):
    queue_input = queue.Queue()
    queue_intermediate = queue.Queue()
    queue_output = queue.Queue()

    # 读取输入文件
    with open(input_path, 'r') as f:
        for line in f:
            queue_input.put(line.strip())


    # 创建并启动Mapper进程
    map_processes = [multiprocessing.Process(target=worker_map, args=(queue_input, queue_intermediate)) 
                     for _ in range(num_workers)]
    for p in map_processes:
        p.start()

    # 等待所有Mapper进程完成
    for p in map_processes:
        p.join()

    # 创建并启动Reducer进程
    reduce_processes = [multiprocessing.Process(target=worker_reduce, args=(queue_intermediate, queue_output))
                        for _ in range(num_workers)]
    for p in reduce_processes:
        p.start()

    # 等待所有Reducer进程完成
    for p in reduce_processes:
        p.join()

    # 输出结果到文件
    with open(output_path, 'w') as f:
        while not queue_output.empty():
            key, count = queue_output.get()
            f.write(f'{key}\t{count}\n')


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

    main(input_path, output_path)
