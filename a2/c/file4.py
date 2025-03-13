import os
import sys
sys.path.append(os.getcwd())
from a2.b import func2,S
def func4():
    print("This is fun4!")
func4()
func2()
s = S(2,3)
print(s.sum())