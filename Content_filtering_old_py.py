import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.stem import WordNetLemmatizer
import psycopg2
#import mysql.connector
from pandas import DataFrame
import numpy as np
import re as re
import swifter
import time
import logging
import math
from wordsegment import load,segment
load()

Postgreserver = 'server1'
Postgredbname = 'db1'
Postgreuser = 'user1'
Postgrepassword= 'password1'

# Step 2: Import data and clean data

gpdbconn = psycopg2.connect(host=Postgreserver, dbname=Postgredbname, user=Postgreuser, password=Postgrepassword)
gpdbcursor = gpdbconn.cursor()
gpdbcursor.execute("""select SKU, product_attributes, city_tier, point_bucket, qtylast12m, qtylast12m from db.merchandise_new_product_attributes where redemption_status in ('not redeemed and new','redeemed and new')
""")
#rows = gpdbcursor.fetchall()
 
df = DataFrame(gpdbcursor.fetchall(),columns=['SKU', 'product_attributes', 'city_tier', 'point_bucket', 'qtylast12m','qtylast12m'])
gpdbcursor.close()
gpdbconn.close()
print('1')
# combined features

def combine_features(row):
    try:
        return row[1]+" "+row[2]+" "+row[3]+" "+row[4]+" "+row[5]
    except:
        print("Error:", row)

stop_words = stopwords.words('english')
from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer = WordNetLemmatizer()
df_new = df.copy()
df_new['index'] = df_new.index 
df_new["features"] = df_new.apply(combine_features,axis=1)
df_new["features"] = df_new["features"].apply(lambda x: [re.sub('[\d\W_]',' ',x)])
df_new["features"] = df_new["features"].apply(lambda x: [word.upper() for word in x])
df_new["features"]=df_new["features"].apply(lambda x: [word for word in x if word not in stopwords.words('english')])
df_new["features"]=df_new["features"].apply(lambda x: [word for word in x if len(word) > 2])
df_new["features"]=df_new["features"].apply(lambda x: [wordnet_lemmatizer.lemmatize(word,pos = 'v') for word in x])
df_new["features"] = df_new["features"].apply(lambda x: [word.upper() for word in x])
df_new["features"] = df_new["features"].apply(lambda x: x[0].split())
df_new["features"]= df_new["features"].apply(lambda x: ' '.join(x))
products = list(df_new["features"])
print('2')
#print(time.ctime())
#df_new1["combine_features"]=df_new1["combine_features"].swifter.set_npartitions(500).apply(lambda x: [word for word in segment(x[0])])
#df_new["features"]=df_new["features"].apply(lambda x: [','.join(segment(word)) for word in x if word not in ['SIXQTY', 'SIXQTYYYYY', 'SIXQTYYYYYYYYYY', 'SIXQTYYYYYYYYYYYYY', 'SIXQTYYYYYY', 'SIXQTYY', 'SIXQTYYYY', 'SIXQTYYYYYYYYYYYY', 'SIXQTYYYYYYY', 'SIXQTYYYYYYYY', 'SIXQTYYY', 'SIXQTYYYYYYYYY', 'SIXQTYYYYYYYYYYY','QTYY', 'QTYYYYYYYYYY', 'QTY', 'QTYYYYYYYYY', 'QTYYYYY', 'QTYYY', 'QTYYYYYYY', 'QTYYYYYY', 'QTYYYY', 'QTYYYYYYYY', 'QTYYYYYYYYYYY', 'QTYYYYYYYYYYYY', 'POINTSS', 'POINTSSSS', 'POINTSSSSSSSS', 'POINTSSSSSSSSS', 'POINTSSS', 'POINTSSSSSS', 'POINTSSSSSSS', 'POINTSSSSSSSSSS', 'POINTSSSSS', 'POINTS','CITYYY', 'CITYY', 'CITYYYY', 'CITYYYYY', 'CITYYYYYY']])
#print(time.ctime())

# Step 3: Recommendation Engine
    
cv = CountVectorizer(stop_words = 'english')
count_matrix = cv.fit_transform(products).toarray()
features = cv.get_feature_names()
features = [features[i].upper() for i in range (0,len(features))]
list1= [features[i] for i in range(0,len(count_matrix[0])) if count_matrix[0][i] == 1]
#len(features)
#for i in range(0,len(features)):
#    features[i]=features[i].upper()
print('3')

# Cosine Similarity between these to find how similar they are to each other

cosine_sim = cosine_similarity(count_matrix)
print('4')
def get_index_from_sku(sku):
    return df_new[df_new['SKU']== sku].index.values[0]

def get_sku_from_index(index):
    return df_new[df_new.index== index]["SKU"].values[0]


gpdbconn = psycopg2.connect(host=Postgreserver, dbname=Postgredbname, user=Postgreuser, password=Postgrepassword)
gpdbcursor = gpdbconn.cursor()
gpdbcursor.execute("""truncate db.merchandise_content_oldredeemers;""")
for index in df_new.index:
    if index%1000==0:
        print('recommendation code processing for product',index)
    similar_products =  list(enumerate(cosine_sim[index]))
    sorted_similar_products = sorted(similar_products,key=lambda x:x[1],reverse=True)
    SKU = get_sku_from_index(index)
    
    
    i=0
    similar_prod=[]
    for element in sorted_similar_products:
        product_to_be_inserted = get_sku_from_index(element[0])
        score = element[1]
        if (i>19):
            break;        
        gpdbcursor.execute("""insert into db.merchandise_content_oldredeemers(SKU, recommended_SKU, score) 
                             values(%s,%s,%s);""" ,(SKU,product_to_be_inserted,score,))
        gpdbconn.commit()
        similar_prod.append(get_sku_from_index(element[0]))
        i=i+1
gpdbcursor.close()
gpdbconn.close()
         

########################################################################################################################################################

