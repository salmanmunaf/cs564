import sys
from json import loads
from re import sub
import os

tempset = set()
with open("user.dat",'r') as f:
    for line in f:
        tempset.add(line)
print("The length of set is:", len(tempset))