# -*- coding: utf-8 -*-
# 脚本设置使用utf-8编码

from datetime import datetime
from glob import glob
import json
from enum import Enum
import os
import schema
import hashlib
import logging
import pandas as pd
import chardet

SOURCE = 'Telechat'
PRETRAIN_CUR_TITLE =''
OUTPUT_DIR = ''
CUR_TITLE = ''

# 配置日志记录
logging.basicConfig(
    filename= SOURCE + '_log_file.log',  # 指定日志文件的名称
    level=logging.INFO,  # 指定日志级别（INFO、WARNING、ERROR、CRITICAL等）
    format='%(asctime)s [%(levelname)s]: %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S'  # 日期和时间格式
)

# 定义一个枚举类
class Json_str(Enum):
    JSON_START = "{"
    JSON_END = '},'
    NONE = ''

# 处理 telechat 的数据
def telechat_extract(f_path, output_file, type_str, max_size):
    if type_str == 'telechat':
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        process_telechat_text(f_path, output_file, max_size)
    else:
        logging.error(f"type: {type_str} not support!")

def process_telechat_text(f_path, output_file, max_size):
    with open(f_path, 'r', encoding='utf-8') as f:
        # 读取第一行
        line = f.readline().strip('\n').replace('\ufeff', '')
        # 判断第一行是否为一个有效的json
        try:
            json_data = json.loads(line)
            # 按jsonl格式处理文件
            process_text_file_common(f_path, output_file, max_size)
        except json.JSONDecodeError:
            line = f.readline()
            logging.warning("JSONDecodeError")
            logging.warning(f"Line 1, error!")
            logging.warning(f"File {f_path} is not a valid json file!")

def process_text_file_common(f_path, output_file, max_size):
    # 文件序号
    file_number = 1
    write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    with open(f_path, 'r', encoding='utf-8') as f:
        for line_number, line in enumerate(f, start=1):
            # 打印迭代信息
            logging.debug(f"Line {line_number}: {line}")
            try:
                json_data = json.loads(line)
            except json.JSONDecodeError or KeyError:
                logging.error("JSONDecodeError")
                logging.error(f"Line {line_number}, error!")
                exit()
            process_text_json_common(json_data, line_number, write_file, f_path)
            # 如果文件超过500M，就关闭文件，新建文件
            if write_file.tell() > max_size:
                write_file.close()
                file_number += 1
                write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')

def process_text_json_common(json_data, line_number, write_file=None, f_path=None):
    logging.debug(f"Processing line {line_number}")
    logging.debug(f"Processing file {f_path}")
    logging.debug(f"Processing json_data {json_data}")
    # 计算json_datas的总大小
    size = 0
    paragraphs = []
    max_paragraph_length = 0
    paragraph_count = 0
    distinct_paragraph_count = 0
    # 生成当前yyyymmdd格式的时间
    time = datetime.now().strftime("%Y%m%d")
    para_md5_set = set()
    logging.debug(json_data)
    json_str = json.dumps(json_data, ensure_ascii=False).encode('utf-8')
    logging.debug(f"Processing json_data {json_str}")
    size = len(json_str)
    content = json_data['data']
    # 通过换行符来拆分content，计算每一段的MD5
    paras = content.split('\n')
    logging.debug(content)
    for para_str in paras:
        para_md5 = hashlib.md5(json.dumps(para_str, ensure_ascii=False).encode('utf-8')).hexdigest()
        # 如果MD5不在集合中，就加入集合
        if para_md5 not in para_md5_set:
            para_md5_set.add(para_md5)
            distinct_paragraph_count += 1
        paragraph_count += 1
        if len(para_str) > max_paragraph_length:
            max_paragraph_length = len(para_str)
        # 生成json
        para_json_schema = schema.TelechatTextParagraphSchema(paragraph_count, para_md5, para_str, '').to_json()
        paragraphs.append(para_json_schema)
    json_schema = schema.TelechatTextSchema(os.path.basename(f_path)+str(line_number), size, max_paragraph_length, paragraph_count, distinct_paragraph_count, '', time, paragraphs)
    # 生成json
    json_str = json_schema.to_json()
    write_file.write(json_str)
    write_file.write('\n')
    return True

def add_files_to_list(path, file_paths):
    if os.path.isfile(path):
        file_paths.append(path)
    elif os.path.isdir(path):
        for item in os.listdir(path):
            add_files_to_list(os.path.join(path, item), file_paths)

def get_encoding(file_path):
    detector = chardet.UniversalDetector()

    with open(file_path, 'rb') as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
        detector.close()

    return detector.result['encoding']

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse Telechat data.")
    parser.add_argument("source_files",type=str, default="final_data_sample_230706test.json", help="文件名或者目录名")
    parser.add_argument("dest_dir",type=str, default="final_data_sample_230706test", help="文件名或者目录名")
    parser.add_argument("-s","--max_size",type=int,default=500 * 1024 * 1024,help="max chunk size")
    # type的类型
    # telechat: 用于telechat的数据
    parser.add_argument("-m","--type",type=str,default='telechat',help="multi model parse")
    args = parser.parse_args()
    # 当前时间
    cur_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_paths = []
    OUTPUT_DIR = args.dest_dir
    add_files_to_list(args.source_files, file_paths)
    # 打印处理清单
    logging.info(f"Processing {len(file_paths)} files")
    logging.info(f"Processing {args.type} data")
    for f_path in file_paths:
        logging.info(f"Processing {f_path}")
        # 检查文件是否存在
        if os.path.isfile(f_path):
            _, ext = os.path.splitext(f_path)
            if ext not in ['.json', '.jsonl']:
                logging.warning(f"Skipping {f_path}")
                continue
        else:
            logging.warning(f"Skipping {f_path}")
            continue
        # 处理提取逻辑
        f_prefix_name = os.path.basename(f_path).split('.')[0]
        output_file_prefix = f'{SOURCE}_{args.type}_{f_prefix_name}_{cur_time}'
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        telechat_extract(f_path, output_file_prefix, args.type, args.max_size)
 