import gzip
import json
import os


def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)  # see below for Python 2.x
        else:
            yield entry.path

total_c = 0
d_s = set()
for file_path in scantree('/Users/bing.he/Workspace/dataspin/tmp/target/'):
    if 'index' in file_path:
        continue
    # if 'test_4' not in file_path:
    #     continue
    print(file_path)
    with open(file_path) as f:
        for line in f:
            d = json.loads(line)
            if d.get('event_id'):
                d_s.add(d.get('event_id'))
            total_c = total_c +1

print(total_c)
print(len(d_s))

total_c = 0
d_s = set()
for file_path in scantree('/Users/bing.he/Workspace/dataspin/temp/'):
    if 'index' in file_path:
        continue
    if 'test_4' not in file_path:
        continue
    print(file_path)
    with gzip.open(file_path, mode='rt', encoding='utf-8') as gdata:
        for line in gdata:
            d = json.loads(line)
            if d.get('event_id'):
                d_s.add(d.get('event_id'))
            total_c = total_c +1

print(total_c)
print(len(d_s))