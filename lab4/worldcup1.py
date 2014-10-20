import re

f = open("worldcup.txt","r")
f.readline()

line = f.readline().strip()
while line!="|}":
    line = f.readline().strip()
    for i in range(4):
        position = re.findall("\|\d{4}]]", f.readline().strip())
        for pos in position:
            out = []
            out.append(line.split("{{fb|")[1].split("}}")[0])
            out.append(pos[1:-2])
            out.append(i+1)
            print(', '.join(map(str,out)))
    f.readline()
    line = f.readline().strip()
f.close()
