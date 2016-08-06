
def log2(n):
    if n == 0:
        return 0
    r = 1
    while n>>1!=0:
        n >>=1
        r+=1
    return r

def num1(n):
    r = 0
    while n!=0:
        if n&1:
            r+=1
        n >>=1
    return r

def lead0(n):
    r=0
    while n>>1<<1 == n and n:
        n>>=1
        r+=1
        

    return r
        


def nex(n):
    if n==0:
        return 1
    if (n&(n-1)) == 0:
        return log2(n)+1
    if num1(n)&1:
        return 2 + lead0(n)
    else:
        return 1


a=set()
b=set()
c = 0
for i in range(1<<17):
    a.add(c)
    b.add(i)
    c = c^(1<<nex(c)-1)
print(a==b)

