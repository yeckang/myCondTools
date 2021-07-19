import csv

fedIDQC8  = 1472
#sec = [1,27,28,29,30]
layer = [1,2]

amcNumV3 = [2,4,6]

sec = [[1,3,5,7,9] ,[11,13,15,17,19], [21,23,25,27,29]]
outF = csv.writer(open("chamberMapQC8.csv","w"))

vfatVer = 3
for i, x in enumerate(amcNumV3):
  gebId = 2
  for sec_ in sec[i]:
    for layer_ in layer:
      fedId = fedIDQC8
      amcNum = amcNumV3
      tmp = [fedId, x, gebId, 1, 1, layer_, sec_, vfatVer]
      #tmp = [fedId, amcNum, gebId, -1, 1, layer_, sec_, vfatVer]
      outF.writerow(tmp)
      gebId+=1


