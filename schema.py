import json
from datetime import datetime

class TelechatTextSchema:

    
    def __init__(self, file_name, file_size, max_paragraph_length, paragraph_count, distinct_paragraph_count, extended_field, time, paragraps):
        self.file_name = file_name
        self.file_size = file_size
        # 新增最长段落数、去重段落数、低质量段落数
        self.max_paragraph_length = max_paragraph_length
        self.paragraph_count = paragraph_count
        self.distinct_paragraph_count = distinct_paragraph_count
        self.extended_field = extended_field
        self.time = time
        self.paragrap = paragraps

    def to_json(self):
        data = {
                "文件名": self.file_name,
                "是否待查文件": False,
                "是否重复文件": False,
                "文件大小": self.file_size,
                "simhash": 0,
                "最长段落长度": self.max_paragraph_length,
                "段落数": self.paragraph_count,
                "去重段落数": self.distinct_paragraph_count,
                "低质量段落数": 0,
                '扩展字段': self.extended_field,
                '时间': self.time, # 格式 yyyymmdd
                "段落": self.paragrap
            }
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

# TelechatText段落Schema
class TelechatTextParagraphSchema:

    def __init__(self, line_number, md5, content, extended_field):
        self.line_number = line_number
        self.md5 = md5
        self.content = content
        self.extended_field = extended_field

    def to_json(self):
        data = {
                    '行号': self.line_number,
                    '是否重复': False,
                    '是否跨文件重复': False,
                    'md5': self.md5, #整行json的md5
                    '内容': self.content, # 对应content
                    '扩展字段': self.extended_field # 对应content
                }
        # return json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        return data
