from bs4 import BeautifulSoup
import re
import string
import binascii
import time
import os
import os.path
import csv


# html parser
def HtmlParser(data,tag):
    soup = BeautifulSoup(data,'html.parser')
    list = soup.find_all(tag)
    return list

# parser the documents. (remove "<body>,</body>, /n" and change to lower.)
def Parser(line):
    document = []
    for x in line:
        document.append( re.findall('[a-zA-z]+', str(line)))
        return document

# global variable.
all_data = []
data_dir = '../hw3/data/'

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
    end_time = time.time()
    print('Done.')
    print('Running time:',end_time-start_time)
