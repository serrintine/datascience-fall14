f = open("cmsc.txt","r")

line = f.readline().strip()
while line != '':
    out = []
    out.append(line)
    line = f.readline().strip()
    while line != '':
        out.append(line)
        out.append(f.readline().strip())
        line = f.readline().strip().split(": ")
        out.append(line[1].split(",")[0])
        out.append(line[2].split(",")[0])
        out.append(line[3].split(")")[0])
        line = f.readline().strip().split()
        out.append(line[0])
        out.append(' '.join(line[1:]))
        line = f.readline().strip().split()
        out.append(line[0])
        out.append(line[1])
        print(', '.join(map(str,out)))
        line = f.readline().strip()
    line = f.readline().strip()
f.close()
