
paints= []
colors= 2
for col in range(colors):
    paints.append([])

def check(i,col ,paints):
    print (paints)
    for pos, n1 in enumerate(paints[col]):
        for n2 in paints[col][pos:]:
            if i== n1+n2:
                return (n1,n2)
    return False
max_num = 0
def paint(i,col,colors):
    a = check(i,col,paints)
    #print ("Number %s Color%s" %(i,col))
    if a:
        global max_num
        if i > max_num:
            max_num = i
        print ("%s %s" %(i, a))
        #paints[col].pop()

        return
    paints[col].append(i)
    for c in range(colors):
        paint(i+1, c, colors)
    paints[col].pop()
paint(1,0,colors)
print("max num "+str(max_num))
