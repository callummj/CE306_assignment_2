relevantDocs = [
"Barney Oldfield's Race for a Life",
"Gentlemen of Nerve",
"Mabel at the Wheel",
"Mickey",
"The Roaring Road",
"The Lucky Devil",
"Tramp, Tramp, Tramp",
"The Yankee Clipper",
]
index1 = [
"Barney Oldfield's Race for a Life",
"Gentlemen of Nerve",
"The Lucky Devil",
"Tramp, Tramp, Tramp",
"The Yankee Clipper",
"The Roaring Road",
"The Crimson City",
"Mabel at the Wheel",
"Nanook of the North",
"Number, Please?",

]

index2 = [
"Brown of Harvard",
"Barney Oldfield's Race for a Life",
"Gentlemen of Nerve",
"Mabel at the Wheel",
"The Wrath of the Gods",
"Birth of a Nation",
"The Goddess of Lost Lake",
"Hearts of the World",
"Mickey",
"The Homesteader",

]

total = 0 # the amount of documents which are relevant in the whole result
for i in index1:
    if i in relevantDocs:
        total+=1

# Calculate the recall of Index 1 and Index 2 by
#
# the amount of documents that they returned which were relevant
# ---------------------------------------------------------------
#               number of relevant docs in total


print("index 1: ", total,"/",len(relevantDocs),"=",total/len(relevantDocs))

total = 0
for i in index2:
    if i in relevantDocs:
        total += 1

print("index 2: ", total,"/",len(relevantDocs),"=",total/len(relevantDocs))