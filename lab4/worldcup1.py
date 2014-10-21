import re

f = open('worldcup.txt','r')
print('Country, Year, Title')
f.readline()
f.readline()
line = f.readline().strip()
while line:
    for i in range(4):
        position = re.findall('\|\d{4}]]', f.readline().strip())
        for pos in position:
            out = []
            out.append(re.search('[A-Z]{3}', line).group(0))
            out.append(pos[1:-2])
            out.append(i+1)
            print(', '.join(map(str,out)))
    f.readline()
    f.readline()
    line = f.readline().strip()
f.close()
