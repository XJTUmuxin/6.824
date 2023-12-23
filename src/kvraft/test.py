import os

def find_last_non_pass_file(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r') as f:
                lines = f.readlines()
                if lines and lines[-1].strip() != "PASS":
                    print(f"Last line of {file_path} is not 'PASS'")

# 设置要遍历的目录
directory = "/home/muxin/6.824/src/kvraft"

# 调用函数查找最后一行不为 "PASS" 的文件
find_last_non_pass_file(directory)