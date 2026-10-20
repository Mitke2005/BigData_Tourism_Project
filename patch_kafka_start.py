import re

path = 'D:/BigData/kafka/bin/windows/kafka-server-start.bat'
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False
for line in lines:
    if 'rem detect OS architecture' in line:
        new_lines.append('    set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G\n')
        skip = True
        continue
    if skip:
        if line.strip() == ')':
            skip = False
        continue
    new_lines.append(line)

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('kafka-server-start.bat patched successfully')
