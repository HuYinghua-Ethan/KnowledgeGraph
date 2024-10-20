import re
import json
import pandas
import itertools
from py2neo import Graph
from collections import defaultdict


'''
使用文本匹配的方式进行知识图谱的使用

re.findall 查找所有匹配的实体
如果 self.entity_set 是 {'apple', 'banana', 'orange'}
则 "|".join(self.entity_set) 生成的字符串将是 'apple|banana|orange'。


import itertools
itertools.combinations(...)：

itertools.combinations(iterable, r) 生成一个包含从可迭代对象 iterable 中选择 r 个元素的所有可能组合的迭代器。
组合的顺序不重要，并且不允许重复选择同一个元素。

# 示例字典
info = {
    'fruits': ['apple', 'banana', 'orange', 'pear']
}

key = 'fruits'
required_count = 2

# 生成所有组合
combinations = list(itertools.combinations(info[key], required_count))

print(combinations)  # 输出: [('apple', 'banana'), ('apple', 'orange'), ('apple', 'pear'), ('banana', 'orange'), ('banana', 'pear'), ('orange', 'pear')]
info[key] 取得的是 ['apple', 'banana', 'orange', 'pear']。
required_count 是 2，表示我们想要生成所有可能的两两组合。
调用 itertools.combinations(info[key], required_count) 将生成以下组合：
('apple', 'banana')
('apple', 'orange')
('apple', 'pear')
('banana', 'orange')
('banana', 'pear')
('orange', 'pear')


itertools.product(*slot_values)
星号（*）用于解包 slot_values 中的每个元素。
例如，如果 slot_values 是 [['red', 'blue'], ['small', 'large']]，那么 *slot_values 将会传递两个参数给 itertools.product：第一个参数是 ['red', 'blue']，第二个参数是 ['small', 'large']。

import itertools

# 示例 slot_values
slot_values = [['red', 'blue'], ['small', 'large']]

# 生成所有组合
value_combinations = list(itertools.product(*slot_values))

print(value_combinations)



'''

class GraphQA:
    def __init__(self):
        self.graph = Graph("neo4j://localhost:7687", user="neo4j", password="neo4j.")
        schema_path = "kg_schema.json"
        templet_path = "question_templet.xlsx"
        self.load(schema_path, templet_path)
        print("知识图谱问答系统加载完毕！\n===============")

    def load(self, schema_path, templet_path):
        self.load_kg_schema(schema_path)
        self.load_question_templet(templet_path)
        return
    
    # 加载 实体、关系、标签、属性 信息
    def load_kg_schema(self, path):
        with open(path, encoding="utf8") as f:
            schema = json.load(f)
        self.relation_set = set(schema["relations"])
        self.entity_set = set(schema["entitys"])
        self.label_set = set(schema["labels"])
        self.attribute_set = set(schema["attributes"])
        return

    # 加载问题模板信息
    def load_question_templet(self, templet_path):
        dataframe = pandas.read_excel(templet_path)
        self.question_templet = []
        for index in range(len(dataframe)):
            question = dataframe["question"][index]
            cypher = dataframe["cypher"][index]
            cypher_check = dataframe["check"][index]
            answer = dataframe["answer"][index]
            self.question_templet.append([question, cypher, json.loads(cypher_check), answer])
        return

    # 获取问题中谈到的 实体 ，可以使用基于词表的方式，也可以使用NER模型
    def get_mention_entitys(self, sentence):
        return re.findall("|".join(self.entity_set), sentence)   # [实体,实体,实体 ...]

    # 获取问题中谈到的 关系， 也可以使用各种文本分类模型
    def get_mention_relations(self, sentence):
        return re.findall("|".join(self.relation_set), sentence)  # [关系, 关系, 关系 ...]

    # 获取问题中谈到的 属性
    def get_mention_attributes(self, sentence):
        return re.findall("|".join(self.attribute_set), sentence)  # [属性, 属性, 属性 ...]

    # 获取问题中谈到的 标签
    def get_mention_labels(self, sentence):
        return re.findall("|".join(self.label_set), sentence)     # [标签, 标签, 标签 ...]

    def parse_sentence(self, sentence):
        entitys = self.get_mention_entitys(sentence)
        relations = self.get_mention_relations(sentence)
        labels = self.get_mention_labels(sentence)
        attributes = self.get_mention_attributes(sentence)
        return {"%ENT%":entitys,
                "%REL%":relations,
                "%LAB%":labels,
                "%ATT%":attributes}

    def check_cypher_info_valid(self, info, cypher_check):
        """
        验证从文本种提取到的信息是否足够填充模板，如果不足够就跳过，节省运算速度
        假设：模板是 %ENT%和%ENT%是什么关系？ 这句话需要两个实体才能填充，如果问题中只有一个，该模板无法匹配
        """
        for key, required_count in cypher_check.items():
            if len(info.get(key, [])) < required_count:
                return False
        return True
    
    # 将提取到的值分配到键上
    def decode_value_combination(self, value_combination, cypher_check):
        res = {}
        for index, (key, required_count) in enumerate(cypher_check.items()):
            if required_count == 1:
                res[key] = value_combination[index][0]
            else:
                for i in range(required_count):
                    key_num = key[:-1] + str(i) + "%"
                    res[key_num] = value_combination[index][i]
        return res

    def get_combinations(self, cypher_check, info):
        """
        对于找到了超过模板中需求的实体数量的情况，需要进行排列组合
        info:{"%ENT%":["周杰伦", "方文山"], “%REL%”:[“作曲”]}
        """
        slot_values = []
        """
            生成 sot_values
            第一次迭代 (key = '%ENT%', required_count = 1):
            info[key] = ["周杰伦", "方文山"]
            itertools.combinations(["周杰伦", "方文山"], 1) 生成的组合：('周杰伦',) ('方文山',) 关于后面为什么有逗号, 是因为
            当你定义一个只有一个元素的元组时，必须在元素后面添加逗号。例如，('周杰伦',)表示一个包含单个元素'周杰伦'的元组。
            如果没有逗号，Python将把('周杰伦')视为一个字符串，而不是元组。
            第二次迭代 (key = '%REL%', required_count = 1):
            info[key] = ["作曲"]
            itertools.combinations(["作曲"], 1) 生成的组合：
            ('作曲',)
            现在，slot_values = [(('周杰伦',), ('方文山',)), (('作曲',))]

            value_combinations是 = itertools.product(*slot_values)
            value_combinations是 = (('周杰伦',), ('作曲',)), (('方文山',), ('作曲',))
            combinations [{'%ENT%': '周杰伦', '%ATT%': '作曲'}, {'%ENT%': '方文山', '%ATT%': '作曲'}]
            """
        for key, required_count in cypher_check.items():
            slot_values.append(itertools.combinations(info[key], required_count))
        value_combinations = itertools.product(*slot_values)
        combinations = []
        for value_combination in value_combinations:
            combinations.append(self.decode_value_combination(value_combination, cypher_check))
        return combinations

    def replace_token_in_string(self, string, combination):
        """
        将带有token的模板替换成真实词
        string: %ENT1%和%ENT2%是%REL%关系吗
        combination: {"%ENT1%":"word1", "%ENT2%":"word2", "%REL%":"word"}
        """
        for key, value in combination.items():
            string = string.replace(key, value)
        return string

    def expand_templet(self, templet, cypher, cypher_check, info, answer):
        """
        对于单条模板，根据抽取到的实体属性信息扩展，形成一个列表
        info:{"%ENT%":["周杰伦", "方文山"], “%REL%”:[“作曲”]}
        就是可能问题是 周杰伦和方文山的作曲是什么， 由此抽出了两个实体，分别是周杰伦和方文山
        """
        combinations = self.get_combinations(cypher_check, info)  # [{'%ENT%': '周杰伦', '%ATT%': '作曲'}, {'%ENT%': '方文山', '%ATT%': '作曲'}]
        templet_cpyher_pair = []
        for combination in combinations:
            replaced_templet = self.replace_token_in_string(templet, combination)
            replaced_cypher = self.replace_token_in_string(cypher, combination)
            replaced_answer = self.replace_token_in_string(answer, combination)
            templet_cpyher_pair.append([replaced_templet, replaced_cypher, replaced_answer])
        return templet_cpyher_pair
            
         
    def expand_question_and_cypher(self, info):
        """
        info: 用户问题的实体、关系、标签、属性
        根据用户问题的实体、关系、标签、属性等信息，扩展出可能的模板和 cypher 语句
        """
        templet_cypher_pair = []
        # question_templet: question_templet.xlsx中构建的问题模板  [question, cypher, json.loads(cypher_check), answer]
        for templet, cypher, cypher_check, answer in self.question_templet:
            # 验证从文本种提取到的信息是否足够填充模板
            if self.check_cypher_info_valid(info, cypher_check):
                templet_cypher_pair += self.expand_templet(templet, cypher, cypher_check, info, answer)
        return templet_cypher_pair

    # 距离函数，文本匹配的所有方法都可以使用
    def sentence_similarity_function(self, string1, string2):
        # print("计算  %s %s"%(string1, string2))
        jaccard_distance = len(set(string1) & set(string2)) / len(set(string1) | set(string2))
        return jaccard_distance


    # 通过问题匹配的方式确定匹配的 cypher 语句
    def cypher_match(self, sentence, info):
        templet_cypher_pair = self.expand_question_and_cypher(info)
        # print(templet_cypher_pair)
        result = []
        for templet, cypher, answer in templet_cypher_pair:
            score = self.sentence_similarity_function(sentence, templet)
            # print(sentence, templet, score)
            result.append([templet, cypher, score, answer])
        result = sorted(result, reverse=True, key=lambda x: x[2])
        return result

    def parse_result(self, graph_search_result, answer, info):
        graph_search_result = graph_search_result[0]
        # print(graph_search_result)
        # input()
        #关系查找返回的结果形式较为特殊，单独处理
        if "REL" in graph_search_result:
            graph_search_result["REL"] = list(graph_search_result["REL"].types())[0]
        answer = self.replace_token_in_string(answer, graph_search_result)
        return answer
        
    def query(self, sentence):
        print("==================")
        print(sentence)  # 打印用户要查询的问题
        info = self.parse_sentence(sentence)  # 将用户问题的实体、关系、标签、属性等信息解析出来，返回一个字典
        # print("info:", info)
        templet_cypher_score = self.cypher_match(sentence, info)  # cypher匹配
        for templet, cypher, score, answer in templet_cypher_score:
            graph_search_result = self.graph.run(cypher).data() # data() 是一个方法，它会将查询的结果以字典列表的形式返回。每个字典对应一个查询结果行，字典的键是结果中返回的字段名，值是对应的结果值。
            # 最高分命中的模板不一定在图上能找到答案, 当不能找到答案时，运行下一个搜索语句, 找到答案时停止查找后面的模板
            if graph_search_result:
                answer = self.parse_result(graph_search_result, answer, info)
                return answer        
        return None



if __name__ == "__main__":
    graph = GraphQA()
    res = graph.query("谁导演的不能说的秘密")
    print(res)
    res = graph.query("发如雪的谱曲是谁")
    print(res)
    res = graph.query("爱在西元前的谱曲是谁")
    print(res)
    res = graph.query("周杰伦的星座是什么")
    print(res)
    res = graph.query("周杰伦的血型是什么")
    print(res)
    res = graph.query("周杰伦的身高")
    print(res)
    res = graph.query("周杰伦和淡江中学是什么关系")
    print(res)
    
    








