import sys
print(sys.path[0]) # 'E:\\pycharm\\project\\test\\a2\\b'

import os
project_path = os.getcwd()
print(project_path)

from file2 import func2,S
sys.path.append(os.path.join(project_path,'a1'))
from file3 import func3



def func1():
    print("This is fun1!")
func2()
s = S(2,3)
print(s.sum())