from mapper import Mapper
from reducer import Reducer
import pickle
import queue
from collections import defaultdict
import multiprocessing
import sys


def read_file(file_path):
    """
    读取文件
    :param file_path: 文件地址
    :return: 文件内容
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
        return lines


def worker_map(map_input: str, map_output: str):
    """
    map方法，将输入解析为 (xx, 1) 的形式
    :param input_path: ["a bb", "bb cc"]
    :param output_path: [('a', 1), ('bb', 1), ('bb', 1), ('cc', 1)]
    """
    with open(map_output, 'w') as f:
        for line in read_file(map_input):
            line = line.strip()
            for k, v in mapper.map(line):
                f.write(f'{k}\t{v}\n')


def shuffle(map_output: str, reduce_input: str):
    """
    shuffle方法，将map的输出shuffle为 [K2, list(V2)] 的形式
    :param map_output: [('a', 1), ('bb', 1), ('bb', 1), ('cc', 1)]
    :param reduce_input: [('a', [1]), ('bb', [1, 1]), ('cc', [1])]
    """
    shuffle_data = defaultdict(list)
    with open(map_output, 'r') as f:
        for line in f:
            key, value = line.strip().split('\t')
            shuffle_data[key].append(int(value))

    with open(reduce_input, 'w') as f:
        for k, v in shuffle_data.items():
            f.write(f'{k}\t{v}\n')


def worker_reduce(redece_input: queue, reduce_output: queue):
    """
    reduce方法，对shuffle的结果求和输出
    :param redece_input: [('a', [1]), ('bb', [1, 1]), ('cc', [1])]
    :param reduce_output: [('a', 1), ('bb', 2), ('cc', 1)]
    """
    with open(reduce_output, 'w') as f:
        for line in read_file(redece_input):
            key, values = line.strip().split('\t')
            k, v = reducer.reduce(key, eval(values))
            f.write(f'{k}\t{v}\n')


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

    map_input = input_path
    map_output = "../map_output.txt"
    reduce_input = "../reduce_input.txt"
    reduce_output = output_path
    num_workers = 1

    # region
    # print("map_input", read_file(map_input))
    # worker_map(map_input, map_output)
    # print("map_output", read_file(map_output))
    # shuffle(map_output, reduce_input)
    # print("reduce_input", read_file(reduce_input))
    # worker_reduce(reduce_input, reduce_output)
    # print("reduce_output", read_file(reduce_output))
    # endregion

    # 创建并启动Mapper进程
    map_process = multiprocessing.Process(target=worker_map, args=(map_input, map_output))
    map_process.start()
    map_process.join()

    # shuffle数据
    shuffle(map_output, reduce_input)

    # 创建并启动Reducer进程
    reduce_process = multiprocessing.Process(target=worker_reduce, args=(reduce_input, reduce_output))
    reduce_process.start()
    reduce_process.join()

