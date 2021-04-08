
'''
res1 = [
52,
76,
543,
620,
667,
290,
680,
86,
438,
341,


]


res2 = [
38,
52,
76,
86,
105,
108,
212,
218,
224,
278,


]

res3 = res1 + res2

res = set(res3)
print("unique docs: ", len(res))

'''

arr = [
538,
603,
267,
500,
654,
617,
621,
142,
218,
528,
]

fname= "query0Index1.txt"
f = open(fname, "w")
for i in arr:
    # f.write("Title: " + i.title + " | " + " ID: " + i.id + "\n")
    f.write(str(i) + "\n")

f.close()