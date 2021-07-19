import csv

import itertools

a= list(set(list(map(''.join, itertools.combinations(["S","L", "S","L", "S", "L"], 3)))))

s1 = [2,3,5,7,8,9,10,11,13,15]
s2 = [0,1,4,6,12,14]
s3 = [18,19,21,23]
s4 = [16,17,20,22]

l1 = [2,3,4,5,6,7,8,9,10,11,12,13,14,15]
l2 = [0,1]
l3 = [18,19,20,21,22,23]
l4 = [16,17]

chL = [[1,3,5,6,9],[11,13,15,17,19],[21,23,25,27,29]]

for x in a:
  outF = csv.writer(open("vfatTypeListQC8_%s.csv"%x,"w"))
  for i,y in enumerate(chL): 
    for ch  in y:
      for l in [1,2]:
        for v in range(24):
          vt=0
          if x[i] == 'S':
            if v in s1: vt=21	
            if v in s2: vt=22	
            if v in s3: vt=23	
            if v in s4: vt=24	
          if x[i] == 'L':
            if v in l1: vt=21
            if v in l2: vt=22
            if v in l3: vt=23
            if v in l4: vt=24
          ieta = 8-divmod(v,8)[1]
          lphi = divmod(v,8)[0]	
          outF.writerow([vt, 3, 1, 1, l,ch,ieta,lphi,v])
 
