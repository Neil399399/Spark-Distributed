from bs4 import BeautifulSoup
import re
import string
import binascii
import time
import os
import os.path
from datasketch import MinHash


# html parser
def HtmlParser(data,tag):
    soup = BeautifulSoup(data,'html.parser')
    list = soup.find_all(tag)
    return list

# parser the documents. (remove "<body>,</body>, /n" and change to lower.)
def Parser(line):
    document = []
    for x in line:
        document.append(re.sub(' +', ' ', str(x).replace("<body>", "").replace("</body>", "").replace("", "").replace("\n", " ").lower()))
        return document

def single(k,document):
    shingle = []
    hashList = []
    for index in range(0,len(document)-k+1):
        single = document[index:index+k]

        # Hash the shingle to a 32-bit integer.
        crc = binascii.crc32(str.encode(single)) & 0xffffffff
        hashList.append(single)
    return hashList


# Define Matrices
# =============================================================================
# ex:
#               D1 D2 D3 D4 D5 ...
#            s1  1  1  1  0  0 ...
#            s2  0  0  1  1  0 ...
#            s3  1  1  0  1  1 ...
# 
# if found the ssame hash in documents, set the element = 1.
# else set the element = 0.
# =============================================================================
def getTriangleMatrices(hash_lists):
    matrice = []
    row = hash_lists[0]
    for i in range(len(row)):
        temp = []
        for j in range(len(hash_lists)):
            if row[i] in hash_lists[j]:
                element = 1
                temp.append(element)
            else:
                element = 0
                temp.append(element)
        matrice.append(temp)
    return matrice            


# minHash.
def minHash(data1,data2):
    m1 = MinHash()
    m2 = MinHash()
    for d in data1:
        m1.update(d.encode('utf8'))
    for d in data2:
        m2.update(d.encode('utf8'))
    return m1.jaccard(m2)

# global value.
start_time = time.time()
documents_hash_list = []
## data has all the news from each files. Each file news append in one list. all_data[] = 22.
all_data = []


if __name__ == '__main__':
    
    # open file.
    print('Start input file ...')
    for file in os.listdir("data/"):
        if file.endswith(".sgm"):
            filename = os.path.join("data", file)
            f = open(filename, 'rb')
            data = f.read()
            htmlResult = HtmlParser(data,'body')
            all_data.append(htmlResult)
    print('Done.')
            



    # save body contents.
    print('Start do parser and make single ...')
    for sub_data in all_data:
        for each_news in sub_data:
            # do parser.
            document = Parser(each_news)
            # do single. k=10
            result = single(10,document[0])
            documents_hash_list.append(result)
    print('Done.')

    # make matrices.
    print('Start make matrices ...')
    matrice = getTriangleMatrices(documents_hash_list)
    print(len(matrice))
    # print(documents_hash_list[0])

    # A = MinHash(htmlResult[0],htmlResult[1])
    # print(len(documents_hash_list))


    
    

   



