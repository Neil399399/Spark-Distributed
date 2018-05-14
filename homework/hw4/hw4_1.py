from bs4 import BeautifulSoup
import re
import string
import binascii
import time
import os
import os.path
import csv
import sys
import numpy



# html parser
def HtmlParser(data,tag):
    soup = BeautifulSoup(data,'html.parser')
    list = soup.find_all(tag)
    return list

# parser the documents. (remove "<body>,</body>, /n" and change to lower.)
def Parser(allOfData):
    document = []
    for each_data in allOfData:
        for each_news in each_data:
                document.append(re.findall('[a-zA-z]+', str(each_news)))
    return document


# global variable.
all_data = []
all_words = []
data_dir = '../hw3/data/'
TF_matrix = []


if __name__ == '__main__':
    
    # open file.
    print('Start input file ...')
    start_time = time.time()
    for file in os.listdir(data_dir):
        if file.endswith(".sgm"):
            filename = os.path.join(data_dir, file)
            f = open(filename, 'rb')
            data = f.read()
            htmlResult = HtmlParser(data,'body')
            all_data.append(htmlResult)
    end_time1 = time.time()
    print('Done.')


    print('Running time:',end_time1-start_time)

    # all_data have 22 elements. each element is one of the data file.
    # all_words are all of the words in all data.

    parser_result = Parser(all_data)
    for each_new in parser_result:
        for x in each_new:
            if (x not in all_words):
                all_words.append(x)

    print(len(all_words))
    print(len(parser_result))

    # make TF-matrix.
    ## open file.
    file = open('TF-matrix.txt', 'a')
    writer = csv.writer(file)

    print('Start create TF-matrix ...')
    for each_word in all_words:
        temp = []
        for each_new in parser_result:
            value = 0
            for word in each_new:
                if word == each_word:
                    value = value + 1
            temp.append(value)
        # TF_matrix.append(temp)
        writer.writerow(temp)
    end_time2 = time.time()


    print('Done.')
    print('Runnibg time:',end_time2-end_time1)
    # transfer all_words in RDD format.
