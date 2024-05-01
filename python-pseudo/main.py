from mapper import Mapper
from reducer import Reducer
import pickle
from collections import defaultdict
import multiprocessing
import sys


def read_file(file_path) -> list:
    """
    读取文件
    :param file_path: 文件地址
    :return: 文件内容
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
        return lines


def divide_file(content, filenames):
    """
    把文件均匀分成n份，每份送入一个map/reduce，能处理不能均分的情况
    :param content: 输入文件内容
    :param filenames: 要保存的文件的名字
    """
    # 每组map传入的输入数据数量为 文件行数//进程数
    total_lines = len(content)
    lines_per_file = total_lines // num_workers
    extra_lines = total_lines % num_workers

    for i in range(len(filenames)):
        with open(filenames[i], 'w') as f:
            start_line = i * lines_per_file + min(i, extra_lines)  # 对于多余的行数，在前面文件中每个加一行
            end_line = start_line + lines_per_file + (1 if i < extra_lines else 0)
            f.writelines(content[start_line:end_line])


def worker_map(map_input: str, map_output: str):
    """
    map方法，将输入解析为 (xx, 1) 的形式
    :param map_input: ["a bb", "bb cc"]
    :param map_output: [('a', 1), ('bb', 1), ('bb', 1), ('cc', 1)]
    """
    with open(map_output, 'w') as f:
        for line in read_file(map_input):
            line = line.strip()
            for k, v in mapper.map(line):
                f.write(f'{k}\t{v}\n')


def shuffle():
    """
    对map的输出文件进行shuffle和排序后输出，输出为多个reduce输入文件
    """
    shuffle_data = defaultdict(list)
    # 读取每个文件并将键值对添加到字典中
    for filename in map_output_paths:
        with open(filename, 'r') as f:
            for line in f:
                key, value = line.strip().split('\t')
                shuffle_data[key].append(int(value))
    # 将字典内容排序并输出为字符串数组
    shuffle_data_str = [key+'\t'+str(shuffle_data[key])+'\n' for key in sorted(shuffle_data)]
    # 拆分数据
    divide_file(shuffle_data_str, reduce_input_paths)


def worker_reduce(reduce_input: str, reduce_output: str):
    """
    reduce方法，对shuffle的结果求和输出
    :param reduce_input: [('a', [1]), ('bb', [1, 1]), ('cc', [1])]
    :param reduce_output: [('a', 1), ('bb', 2), ('cc', 1)]
    """
    with open(reduce_output, 'w') as f:
        for line in read_file(reduce_input):
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

    # map/reduce进程数量
    num_workers = 3
    # 各阶段文件存储地址
    map_input_paths =     [f"data/map_input_{i}.txt"     for i in range(num_workers)]
    map_output_paths =    [f"data/map_output_{i}.txt"    for i in range(num_workers)]
    reduce_input_paths =  [f"data/reduce_input_{i}.txt"  for i in range(num_workers)]
    reduce_output_paths = [f"data/reduce_output_{i}.txt" for i in range(num_workers)]

    # 拆分输入为 num_workers 个文件
    divide_file(read_file(input_path), map_input_paths)

    # 创建并启动Mapper进程
    map_processes = [multiprocessing.Process(target=worker_map, args=(map_input_paths[i], map_output_paths[i]))
                     for i in range(num_workers)]
    for p in map_processes:
        p.start()
    for p in map_processes:
        p.join()

    # shuffle数据
    shuffle()

    # 创建并启动Reducer进程
    reduce_processes = [multiprocessing.Process(target=worker_reduce, args=(reduce_input_paths[i], reduce_output_paths[i]))
                        for i in range(num_workers)]
    for p in reduce_processes:
        p.start()
    for p in reduce_processes:
        p.join()

    # 合并reduce结果
    with open(output_path, 'w') as f:
        for filename in reduce_output_paths:
            with open(filename, 'r') as rf:
                for s in rf:
                    f.write(s)
