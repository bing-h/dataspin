def topoSort(graph):     
    in_degrees = dict((u,0) for u in graph)   #初始化所有顶点入度为0     
    num = len(in_degrees)     
    for u in graph:         
        for v in graph[u]:
            in_degrees[v] += 1    #计算每个顶点的入度  
    Q = [u for u in in_degrees if in_degrees[u] == 0]   # 筛选入度为0的顶点    
    Seq = []     
    while Q:         
        u = Q.pop()       #默认从最后一个删除         
        Seq.append(u)         
        for v in graph[u]:   
            in_degrees[v] -= 1    #移除其所有出边
            if in_degrees[v] == 0:        
                Q.append(v)          #再次筛选入度为0的顶点
    if len(Seq) == num:       #输出的顶点数是否与图中的顶点数相等
        return Seq     
    else:         
        return None

G = {
    'a':['b','f'],
    'b':['c','d','f'],
    'c':['d'],
    'd':['e','f'],
    'e':['f'],
    'f':[]
}


s = {
        "split by app_id":
            [
                {"build primary key index":"save to index target"},
                {"deduplicate by event_id":["save to target","save to target 1"]}
            ],
        "flatten json":["save to target","save to target 1"]
    }
g = {}
for k,v in s.items():
    print(k)
    if type(v) in (dict):
        pass
    if type(v) in (list):
        for i in v:
            print(i)

# p = {}
# for k,v in s.items():
    

print(topoSort(s))


