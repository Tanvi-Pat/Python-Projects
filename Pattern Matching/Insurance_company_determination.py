#!/usr/bin/env python
# coding: utf-8

# ### Insurance

# In[1]:


from collections import namedtuple
import re
import csv
from pathlib import Path
import csv
import pandas as pd
import string
import os
import xlsxwriter 
from nltk import ngrams
from ngram import NGram
get_ipython().system('pip install wordsegment')
from wordsegment import load, segment
from sys import getsizeof

import yaml
from nltk import ngrams
import pandas as pd
load()

get_ipython().system('pip install jupyternotify')
import jupyternotify
ip = get_ipython()
ip.register_magics(jupyternotify.JupyterNotifyMagics)


# In[3]:


df5=pd.read_csv("Y:\\stuff\\transaction_data.csv") 


# In[4]:


len(df5)


# In[5]:


df5.particulars = df5.particulars.apply(lambda x: str(x).upper())
#gheto.particulars = re.sub(r'\d+', '', str(gheto.particulars))
df5.particulars = df5.particulars.apply(lambda x: x.translate(str.maketrans('', '', string.digits)))
df5.particulars = df5.particulars.apply(lambda x: x.translate(str.maketrans('', '', string.punctuation)))


# In[308]:


type(df6)


# In[177]:


df6 = pd.DataFrame(df5.particulars)


# In[4]:


searchfor = ['INSURANCE','INSU','TRAVEL&INS','HEALTH&IN','CANCER']
df = pd.DataFrame(df5.particulars[df5.particulars.str.contains('|'.join(searchfor))]) #60794


# In[5]:



insu_words_list = []
df.particulars.apply(lambda x: [insu_words_list.append(x) for x in x.split(' ')])
insu_freq_list = [insu_words_list.count(p) for p in insu_words_list]
insu_word_freq= dict(zip(insu_words_list,insu_freq_list))
insu_word_freq_dict = {k: v for k, v in sorted(insu_word_freq.items(), key=lambda item: item[1], reverse = True)}


# In[7]:


searchfor = ['MUTUAL','FUND','YIELD','EQUITY','DEBT','INDEX','DIVIDEND','NIFTY','SENSEX']
df_MF = pd.DataFrame(df5.particulars[df5.particulars.str.contains('|'.join(searchfor))]) #280262


# In[12]:


get_ipython().run_line_magic('who', 'dict')
get_ipython().run_line_magic('timeit', 'mf_words_list = []')


# In[36]:


mf_word_freq_dict


# In[13]:


mf_words_list = []
df_MF.particulars.apply(lambda x: [mf_words_list.append(x) for x in x.split(' ')])
mf_freq_list = [mf_words_list.count(p) for p in mf_words_list]
mf_word_freq= dict(zip(mf_words_list,mf_freq_list))
mf_word_freq_dict = {k: v for k, v in sorted(mf_word_freq.items(), key=lambda item: item[1], reverse = True)}


# In[114]:


#len(mf_word_freq_dict)#7201
#len(insu_word_freq_dict)#3109

def ReadDicttoCSV(csv_file, dict1, dict2):
    with open(csv_file, 'w') as fw: 
        fw.write('insu_word,insu_freq\n')
        for word, freq in dict1.items():
            fw.write(f'{word},{freq}\n')
        
        fw.write('mf_word,mf_freq\n')
        for word, freq in dict2.items():
            fw.write(f'{word},{freq}\n')
     


# In[116]:


csv_file = data_folder1 / 'statement_mf_insu_words1.csv'
dict1 = insu_word_freq_dict
dict2 = mf_word_freq_dict

ReadDicttoCSV(csv_file,dict1,dict2)


# In[173]:


MAX_N_GRAMS = 2


# In[8]:


data_folder = Path('Y:/stuff')
data_folder1 = Path('T:/USERS/Tanvi_Patil/Scrapping')



# In[9]:


Result = namedtuple('Result', ['insurance_company', 'insurance_type','insurance_present'])
Result.__new__.__defaults__ = ('',) * len(Result._fields)


# In[186]:


insurance_company_dict = yaml.load(open(data_folder1 / 'insurance_company.yml'))
insurance_type__dict = yaml.load(open(data_folder1 / 'insurance_type_list.yml'))
#insurance_type_set = set(list(map(lambda x: x.replace('\n', ''), open(data_folder1 / 'insurance_type.txt').readlines())))


# In[174]:


def extract_type(particular,is_segment):
    if is_segment == 0:
        tokens = particular.split()
        for token in tokens:
            for key in insurance_type__dict.keys():
                if NGram.compare(token,key) >= 0.5:
                    return insurance_type__dict[key]
        return ''
    else:
        tokens =segment(particular)
        for token in tokens:
            token = str(token).upper()
            for key in insurance_type__dict.keys():
                if NGram.compare(token,key) >= 0.5:
                    return insurance_type__dict[key]
        return ''


# In[175]:


def extract_company(particular,is_segment):
    if is_segment == 0:
        if particular in insurance_company_dict:
            return insurance_company_dict[particular]
        tokens = particular.split()
        for n in reversed(range(1, min(MAX_N_GRAMS, len(tokens)) + 1)):
            for ngram_token_list in ngrams(tokens, n):
                ngram_token = ' '.join(ngram_token_list)
                if ngram_token in insurance_company_dict.keys():
                    return insurance_company_dict[ngram_token]
                for key in insurance_company_dict.keys():
                    if NGram.compare(ngram_token,key) >= 0.5:
                        return insurance_company_dict[key]
        return ''
    else:
        tokens =segment(particular)
        tokens = [x.upper() for x in tokens]
        for n in reversed(range(1, min(MAX_N_GRAMS, len(tokens)) + 1)):
            for ngram_token_list in ngrams(tokens, n):
                ngram_token = ' '.join(ngram_token_list)
                if ngram_token in insurance_company_dict.keys():
                    return insurance_company_dict[ngram_token]
                for key in insurance_company_dict.keys():
                    if NGram.compare(ngram_token,key) >= 0.5:
                        return insurance_company_dict[key]
        return ''


# In[176]:


def preprocess(particular):
    particular = str(particular).upper()
    particular = re.sub('[^A-Z\d\s]',' ',particular)
    particular = re.sub('\s+',' ',particular)
    particular = particular.strip()
    return particular


# In[109]:


particular ='TA REIMBURSEMENT FR INSURANCE TRAINING  AT MARKETYARD HO'
searchfor = ['INSU','BIMA','FAMILY&IN','LIFE&INS','CAR&IN','HOME&IN','WHEELER&INS','TRAVEL&INS','HEALTH&INS','SENIOR&INS','ACCIDENT&INS','MEDICLAIM']
not_include = ['TRAINING','SERVICE']
regex = ('((?=.*'+('.*$)|(?=.*'.join(searchfor))+'.*$))^(?!.*('+'|'.join(not_include)+'))')
#regex = ('.*'+('.*|.*'.join(searchfor))+'.*')
print(regex)
m = re.match(regex,particular)
if m is None:
    print(None)
#^(?!.*STYLE)
#(?=.*MAX.*LIF.*IN.*$)|
##m = re.match('((?=.*MAX.*LIF.*IN.*$)|(?=.*INDIA.*FIRST.*LIF.*$))^(?!.*(STYLE|SCIENCE))','RUPAYECOMWWWICICIPRULIFECOM MUMBAI MHINXXXXXX')


# In[258]:


def clean_particular(particular):
    #particular = preprocess(particular)
    searchfor = ['INSU','BIMA','FAMILY&IN','LIFE&INS','CAR&IN','HOME&IN','WHEELER&INS','TRAVEL&INS','HEALTH&INS','SENIOR&INS','ACCIDENT&INS','MEDICLAIM']
    #regex = ('.*'+('.*|.*'.join(searchfor))+'.*')
    not_include = ['TRAINING','SERVICE']
    regex = ('((?=.*'+('.*$)|(?=.*'.join(searchfor))+'.*$))^(?!.*('+'|'.join(not_include)+'))')
    m = re.match(regex,particular)
    if m is None:
        insurance_present = 'NOT INSURANCE'
        insurance_type = ''
        insurance_company = ''
    else:
        insurance_present = 'INVOLVE INSURANCE'
        insurance_company = extract_company(particular,0)
        insurance_type = extract_type(particular,0)
        if insurance_type == '':
            insurance_type = extract_type(particular,1)
        if insurance_company == '':    
            insurance_company = extract_company(particular,1)        
        
    return Result(insurance_company=insurance_company, insurance_type=insurance_type, insurance_present = insurance_present)


# In[214]:


token= segment('RTGSRELIANCELIFEINSURANCECOLTD HDFC')
token


# In[163]:


clean_particular('Bajaj Allianz Insu Kumar Sharma')


# In[178]:


get_ipython().run_cell_magic('time', '', "df6['particulars'] = df6.particulars.apply(lambda x: preprocess(x))")


# In[154]:


NGram.compare('HEAL','HEALTH')


# In[252]:


sample_particulars = df6.sample(100)


# In[259]:


get_ipython().run_cell_magic('time', '', "df6['clean_particulars'] = df6.particulars.apply(lambda x: clean_particular(x))\n#sample_particulars")


# In[263]:


df6[df6['insurance_company'] != '']


# In[187]:


get_ipython().run_cell_magic('time', '', "df6['insurance_company'] = df6.particulars.apply(lambda x: clean_particular(x).insurance_company)")


# In[188]:


get_ipython().run_cell_magic('time', '', "df6['insurance_type'] = df6.particulars.apply(lambda x: clean_particular(x).insurance_type)")


# In[189]:


get_ipython().run_cell_magic('time', '', "df6['insurance_present'] = df6.particulars.apply(lambda x: clean_particular(x).insurance_present)")


# In[41]:


sample_particulars.insurance_company.isna().any()


# In[200]:


sample_particulars.shape


# In[211]:


df6[df6['insurance_company'] != ''].to_csv('T:\\USERS\\Tanvi_Patil\\Scrapping\\Insurance_accuracy_results\\insurance_company_2.csv')
df6[df6['insurance_type'] != ''].to_csv('T:\\USERS\\Tanvi_Patil\\Scrapping\\Insurance_accuracy_results\\insurance_type_2.csv')


# In[240]:


insurance_df = df6[df6['insurance_present'] == 'INVOLVE INSURANCE']
insurance_df


# In[236]:


df6
searchfor = ['HOME','PERSONAL']
    #regex = ('.*'+('.*|.*'.join(searchfor))+'.*')
    #not_include = ['TRAINING','SERVICE']
regex = ('(?=.*'+('.*$)|(?=.*'.join(searchfor))+'.*$)')
regex
#m = re.match(regex,particular)


# In[237]:


get_ipython().run_cell_magic('time', '', "df6['Loan_present']=df6.particulars.apply(lambda x: 'Not Loan' if re.match(regex,x) is None else 'Loan')")


# In[238]:


df6[df6['Loan_present']=='Loan']


# In[136]:


df.isna().sum()


# In[220]:


NGram.compare('AIA','AIG')


# In[241]:


insurance_words_lis = []
insurance_df.particulars.apply(lambda x: [insurance_words_lis.append(i) for i in x.split(' ')])


# In[242]:


word_freq = [insurance_words_lis.count(p) for p in insurance_words_lis]


# In[243]:


word_list_freq  = dict(list(zip(insurance_words_lis,word_freq)))


# In[249]:


#insurance_words_lis
import matplotlib.pyplot as plt 
get_ipython().system('pip install wordcloud')
from wordcloud import WordCloud, STOPWORDS 


# In[250]:


import matplotlib.pyplot as plt 
from wordcloud import WordCloud, STOPWORDS 

stopwords = set(STOPWORDS)
comment_words = ' '

for i in insurance_words_lis:
    comment_words = comment_words + i + ' '
    


# In[251]:


wordcloud = WordCloud(width = 800, height = 800, 
                background_color ='white', 
                stopwords = stopwords, 
                min_font_size = 10).generate(comment_words)

plt.figure(figsize = (8, 8), facecolor = None) 
plt.imshow(wordcloud) 
plt.axis("off") 
plt.tight_layout(pad = 0) 
  
plt.show() 


# In[171]:


insu_dict = {k: v for k, v in sorted(word_list_freq.items(), key=lambda item: item[1], reverse=True)}


# In[172]:


mutualfund_words = pd.read_csv("T:\\USERS\\Tanvi_Patil\\Scrapping\\MF_Data_Cleaned.csv",header = 0)


# In[212]:


mutual_fund_list = []
mutualfund_words.Mutual_Fund.apply(lambda x: [mutual_fund_list.append(i) for i in x.split(' ')])


# In[174]:


mutual_fund_freq=[mutual_fund_list.count(p) for p in mutual_fund_list]
mutual_funds_freq_dict = dict(list(zip(mutual_fund_list, mutual_fund_freq)))


# In[175]:


mutual_fund_dict = {k:v for k,v in sorted(mutual_funds_freq_dict.items(),key = lambda item: item[1], reverse = True)}


# In[177]:


final_dict_insu = {x:insu_dict[x] for x in insu_dict if x in mutual_fund_dict}
final_dict_mutual = {x:mutual_fund_dict[x] for x in mutual_fund_dict if x in insu_dict}


# In[178]:


import csv
csv_columns = ['identifier','insu_freq','mutual_freq']

try:
    with open('T:\\USERS\\Tanvi_Patil\\Scrapping\\intersection_keywords.csv', 'w',newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for k in final_dict_insu.keys():
            writer.writerow({'identifier':k,'insu_freq':final_dict_insu[k],'mutual_freq':final_dict_mutual[k]})
except IOError:
    print("I/O error")


# In[187]:


final_dict_insu_only = {x:insu_dict[x] for x in insu_dict if x not in mutual_fund_dict}
final_dict_mutual_only = {x:mutual_fund_dict[x] for x in mutual_fund_dict if x not in insu_dict}


# In[211]:


import csv
from itertools import cycle 
csv_columns = ['insu_identifier','insu_freq','mutual_identifier','mutual_freq']

try:
    with open('T:\\USERS\\Tanvi_Patil\\Scrapping\\Only_keywords.csv', 'w',newline='',encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for k,v in zip(final_dict_mutual_only.keys(),cycle(final_dict_insu_only.keys())):
            writer.writerow({'mutual_identifier':k,'mutual_freq': final_dict_mutual_only[k],'insu_identifier':v,'insu_freq':final_dict_insu_only[v]})
except IOError:
    print("I/O error")


# In[297]:


get_ipython().run_line_magic('who', '')


# In[ ]:




