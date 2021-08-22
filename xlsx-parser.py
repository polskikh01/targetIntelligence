import pandas
import numpy as np
import matplotlib.pyplot as plt
import time
import io
from datetime import datetime
import openpyxl
import ast

filename = 'C:\\Users\\AorusGaming\\Desktop\\Target Intelligence\\check.xlsx'
wb = openpyxl.load_workbook(filename=filename)
sheet = wb['DataSet']
new_row = ['Ноябрь 2020', 'Декабрь 2020', 'Январь 2021', 'Февраль 2021', 'Март 2021', 'Апрель 2021', 'Май 2021', 'Июнь 2021']
sheet.append(new_row)

dirsPlayerLog = []
f = open("BigData.txt", "r")
line = f.readline()
while line:
    line = f.readline()
    dirsPlayerLog.append(line.rstrip())
f.close()

del dirsPlayerLog[-1]

for i in range(1,38,2):
    oneMonth = dirsPlayerLog[i]
    my_list = ast.literal_eval(oneMonth)
    list_to_write = []
    for j in range(0,8):
        month = ""
        for k,v in my_list[j].items():
            month += str(v) + " ,"
        list_to_write.append(month[:-1])
    sheet.append(list_to_write)
wb.save(filename)
