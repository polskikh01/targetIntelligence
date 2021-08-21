import pandas
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime

#['AddedOnDate', 'Mac', 'RadarId', 'Face', 'AdmTypeBit', 'AddedOnTick', 'FrameOid']
#['OidColumn', 'PlayerUUID', 'BillingDate', 'MediaItemName', 'Duration', 'BookingId', 'LogFileName', 'Src', 'MediaItemId', 'advertisementId', 'MacCountAll', 'MacCountStaticOnly', 'DateTimeInTick', 'DateTimeOutTick']
colPlayerLog = ['OidColumn', 'MediaItemName', 'advertisementId', 'MacCountAll']
colCrowd = ['AddedOnDate', 'Mac', 'RadarId', 'Face', 'AdmTypeBit', 'AddedOnTick', 'FrameOid']

dirsCrowd = ["E:\dataset\RowData\crowd\player=40\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=257\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=258\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=259\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=260\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=261\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=262\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=263\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=264\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=265\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=267\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=268\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=269\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=270\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=271\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=272\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=274\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=333\month=2020-11.parquet",
        "E:\dataset\RowData\crowd\player=403\month=2020-11.parquet"]

dirsPlayerLog = ["E:\dataset\RowData\player_log\player=26\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=28\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=256\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=257\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=258\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=271\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=278\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=280\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=281\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=282\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=342\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=408\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=411\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=1547\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=1548\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=1571\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=2609\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=2619\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=2629\month=2020-11.parquet",
        "E:\dataset\RowData\player_log\player=2651\month=2020-11.parquet"]


for i in range(0, len(dirsPlayerLog)):
    test2 = pandas.read_parquet(dirsPlayerLog[i], engine = "fastparquet", columns = colPlayerLog)
    print(test2.loc[test2['advertisementId'] == 15715]['MacCountAll'].sum())



#ПРИГОДИТСЯ
#for i in range(0, 100):
#    print(test1.loc[i])

#test2 = pandas.read_parquet(dirs[1], engine = "fastparquet", columns = col2)
#print(test2.loc[0])

#print("\n")

#test1 = pandas.read_parquet("E:\dataset\RowData\player_log\player=257\month=2020-11.parquet", engine = "fastparquet", columns = col1)
#print(test1.loc[0])

#for i in range(0, len(dirs)):
#    test2 = pandas.read_parquet(dirs[i], engine = "fastparquet", columns = col2)
#    print(test2.loc[test2['FrameOid'] == '5088033011'])

#test2 = pandas.read_parquet("E:\dataset\RowData\crowd\player=258\month=2021-6.parquet", engine = "fastparquet", columns = col2)

#print(datetime.utcfromtimestamp(int(test.loc[0]['AddedOnTick'])/1000).strftime('%Y-%m-%d %H:%M:%S'))
#print(test2.loc[])
#print(test2.loc[test2['FrameOid'] == '7358818680'])
#print(test.loc[test['FrameOid'] == test.loc[testFrameOid]['FrameOid']].count()['FrameOid'])












#визуализация
'''
dictionary = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 11: 5, 
              12: 8, 13: 74, 14: 4, 15: 3, 16: 1, 17: 1, 18: 1, 19: 1, 20: 1, 21: 2,22: 10, 23: 100, 24: 24, 25: 26,
              26: 45, 27: 56, 28: 56, 29: 45, 30: 34, 31: 26}
plt.bar(list(dictionary.keys()), dictionary.values(), color='g')

plt.xlabel('Days of Month')
plt.ylabel('OTS')
plt.title('Histogram of OTS per days of mouth')
plt.xlim(0, 31)
plt.ylim(0, 1500)
plt.grid(True)
plt.show()
'''
