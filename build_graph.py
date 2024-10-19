import re
import json
from py2neo import Graph
from collections import defaultdict

'''
读取三元组，并将数据写入neo4j

在图数据库建模中，实体 - 关系 - 实体 和 实体 - 属性 - 属性值 是两种不同的结构，它们分别用于表示不同类型的信息。以下是对这两者的详细区分：

1. 实体 - 关系 - 实体（Entity - Relationship - Entity）
实体（Entity）：表示一个节点，通常是一个对象或概念，例如一个人、地点或事件。
关系（Relationship）：表示两个实体之间的关联，通常用箭头表示。关系可以有方向性，表明一个实体如何与另一个实体相互连接。
示例：
(Emil:Person)-[:FRIENDS_WITH]->(Alice:Person)
在这个示例中：
Emil 和 Alice 是两个实体（节点）。
FRIENDS_WITH 是关系，表示 Emil 和 Alice 之间的友谊。

2. 实体 - 属性 - 属性值（Entity - Attribute - Attribute Value）
实体（Entity）：同样表示一个节点，包含具体的信息。
属性（Attribute）：表示与该实体相关的具体特征或数据项。
属性值（Attribute Value）：是属性的具体数据，描述了该实体的特征。
(Emil:Person {name:"Emil Eifrem", born:1978})


'''


# 连接数据库
graph = Graph("neo4j://localhost:7687", user="neo4j", password="neo4j.")

# 属性字典
attribute_data = defaultdict(dict)
# 关系字典
relation_data = defaultdict(dict)
# 标签字典
label_data = {}


"""
有的实体后面有括号，里面的内容可以作为标签
提取到标签后，把括号部分删除
"""
def get_label_then_clean(x, label_data):
    # 使用 re.search 检查字符串 x 是否包含中文括号 （ 和 ） 之间的内容。
    # 如果找到了这样的内容，就进入下一个步骤。
    if re.search("（.+）", x):
        # 使用 re.search 提取括号内的内容，并将其赋值给 label_string。group() 方法返回找到的匹配字符串。
        label_string = re.search("（.+）", x).group()
        for label in ["歌曲", "专辑", "电影", "电视剧"]:
            if label in label_string:
                # 使用 re.sub 删除 x 中的括号及其内容，清理字符串。
                x = re.sub("（.+）", "", x)
                label_data[x] = label
            else:
                x = re.sub("（.+）", "", x)
    return x


# 读取 实体-关系-实体 三元组文件
with open("triplets_head_rel_tail.txt", encoding="utf8") as f:
    for line in f:
        head, relation, tail = line.strip().split("\t") # 取出三元组
        head = get_label_then_clean(head, label_data)
        relation_data[head][relation] = tail


# 读取 实体-属性-属性值 三元组文件
with open("triplets_enti_attr_value.txt", encoding="utf8") as f:
    for line in f:
        entity, attribute, value = line.strip().split("\t")  # 取出三元组
        entity = get_label_then_clean(entity, label_data)
        attribute_data[entity][attribute] = value


# 构建 cypher 语句
cypher = ""
in_graph_entity = set()

# CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})  实体 - 属性 - 属性值
for i, entity in enumerate(attribute_data):
    # 为所有的实体增加一个名字属性
    attribute_data[entity]["NAME"] = entity
    # 为所有的实体增加一个名字的属性
    text = "{"
    for attribute, value in attribute_data[entity].items():
        text += "%s:\'%s\',"%(attribute, value)
    text = text[:-1] + "}"  #最后一个逗号替换为大括号
    if entity in label_data:
        label = label_data[entity]
        # 带标签的实体构建语句
        cypher += "CREATE (%s:%s %s)" % (entity, label, text) + "\n"
    else:
        # 不带标签的实体构建语句
        cypher += "CREATE (%s %s)" % (entity, text) + "\n"
    in_graph_entity.add(entity)

"""
CREATE (Emil:Person {name:"Emil Eifrem", born:1978})     
CREATE (Emil)-[:ACTED_IN {roles:["Emil"]}]->(TheMatrix)   实体 - 关系 - 实体
"""
# 构建关系语句
for i, head in enumerate(relation_data):
    # 有可能实体只有和其他实体的关系，但没有属性，为这样的实增加一个名称属性，便于在图中认出
    if head not in in_graph_entity:
        cypher += "CREATE (%s {NAME:'%s'})" % (head, head) + "\n"
        in_graph_entity.add(head)
    for relation, tail in relation_data[head].items():
        if tail not in in_graph_entity:
            cypher += "CREATE (%s {NAME:'%s'})" % (tail, tail) + "\n"
            in_graph_entity.add(tail)
        else:
            cypher += "CREATE (%s)-[:%s]->(%s)" % (head, relation, tail) + "\n"


print(cypher)
# input()

# 执行建表脚本
graph.run(cypher)


# 记录我们图谱里都有哪些实体，哪些属性，哪些关系，哪些标签
data = defaultdict(set)
for head in relation_data:
    data["entitys"].add(head)
    for relation, tail in relation_data[head].items():
        data["relations"].add(relation)
        data["entitys"].add(tail)

for enti, label in label_data.items():
    data["entitys"].add(enti)
    data["labels"].add(label)

for enti in attribute_data:
    for attr, value in attribute_data[enti].items():
        data["entitys"].add(enti)
        data["attributes"].add(attr)

data = dict((x, list(y)) for x, y in data.items())  # 把集合转化为列表的形式

with open("kg_schema.json", "w", encoding="utf8") as f:
    f.write(json.dumps(data, ensure_ascii=False, indent=2))




