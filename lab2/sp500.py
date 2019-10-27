file = open('sp500.csv', 'r')
outf = open('sp500.txt', 'w')

cnt = 0
for line in file:
    cnt += 1
    if cnt == 1:
        continue
    data = line.strip().split()
    outf.write(','.join(data) + '\n')
   
file.close()
outf.close()
