import pandas
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime

#['AddedOnDate', 'Mac', 'RadarId', 'Face', 'AdmTypeBit', 'AddedOnTick', 'FrameOid']
col = ['FrameOid', 'Mac', 'AddedOnTick']
test = pandas.read_parquet("E:\dataset\RowData\crowd\player=40\month=2021-1.parquet", engine = "fastparquet", columns = col)

count = 0

for i in range(0,test.count()[0]):
    if test.loc[i].get('FrameOid') == str(5704191933):
        count+=1
        print(test.loc[i].get('FrameOid') + str(" !!!"))
    print(test.loc[i].get('FrameOid'))

print(count)


#визуализация
'''
# Fixing random state for reproducibility
np.random.seed(19680801)

mu, sigma = 100, 15
x = mu + sigma * np.random.randn(10000)

# the histogram of the data
n, bins, patches = plt.hist(x, 50, density=True, facecolor='g', alpha=0.75)

plt.xlabel('OTS')
plt.ylabel('Days of Month')
plt.title('Histogram of OTS per days of mouth')
plt.xlim(40, 160)
plt.ylim(0, 0.03)
plt.grid(True)
plt.show()
'''

#пересчёт datatime
'''
from datetime import datetime
ts = int("1284101485")

# if you encounter a "year is out of range" error the timestamp
# may be in milliseconds, try `ts /= 1000` in that case
print(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
'''
