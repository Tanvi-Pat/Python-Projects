#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd


# In[74]:


df_data = pd.read_csv("C:\\Users\\App_final_dataset.csv")


# In[75]:


df_data.head()


# In[6]:


import sweetviz as sv


# In[40]:


advert_report = sv.analyze(df_data)


# In[41]:


advert_report.show_html('App.html')


# In[63]:


len(df_data.columns)


# In[138]:


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, accuracy_score, recall_score, f1_score
import random
import progressbar
import xgboost as xgb
import numpy as np


# In[46]:


df_data.head()


# In[76]:


df_data.shape


# In[128]:


df_data_X = df_data.drop(columns= ['may_storytrt','appl_identity'])
#df_data_X= df_data[['activities_page_3','activities_page_4','is_apr_subscription_active']]
df_data_Y = df_data['may_storytrt']
print(df_data.shape)
print(df_data_Y.shape)
print(df_data_X.shape)


# In[129]:


X_train, X_test, Y_train, Y_test = train_test_split(df_data_X, df_data_Y, stratify = df_data_Y)


# In[130]:


print(len(X_train))
print(len(X_test))
print(len(Y_train))
print(len(Y_test))
print(Y_test.value_counts())
Y_train.value_counts()


# In[176]:


D_train = xgb.DMatrix(X_train, label = Y_train)
D_test = xgb.DMatrix(X_test, label = Y_test)
param = {'eta':0.3, 'max_depth':6, 'objective':'multi:softprob','num_class':2}
steps = 20


# In[177]:


model = xgb.train(param,D_train,steps)


# In[227]:


preds_train = model.predict(D_train)
print(preds_train)
print(len(preds_train))
best_preds_train = np.asarray([np.argmax(line) for line in preds_train])
print(best_preds_train)


preds_test = model.predict(D_test)
print(preds_test)
best_preds_test = np.asarray([np.argmax(line) for line in preds_test])
print(best_preds_test)
baseline_metric = recall_score(Y_test, best_preds_test, average='macro')


values, counts = np.unique(best_preds_test, return_counts=True)
print(values, counts)
values, counts = np.unique(Y_test, return_counts=True)
print(values, counts)


# In[218]:


print(best_preds_train)
print("Precision = {}".format(precision_score(Y_train, best_preds_train, average='macro')))
print("Recall = {}".format(recall_score(Y_train, best_preds_train, average='macro')))
print("Accuracy = {}".format(accuracy_score(Y_train, best_preds_train)))
print(best_preds_test)
print("Precision = {}".format(precision_score(Y_test, best_preds_test, average='macro')))
print("Recall = {}".format(recall_score(Y_test, best_preds_test, average='macro')))
print("Accuracy = {}".format(accuracy_score(Y_test, best_preds_test)))


# In[247]:


import operator
d = model.get_score()
#print(sorted(dict_score,key = dict_score.__getitem__,reverse = True))
#print(dict_score)
dict_score = dict(sorted(d.items(), key=operator.itemgetter(1),reverse=True))

#dict_score


# import matplotlib.pyplot as plt
# from matplotlib.pyplot import figure

# In[215]:


plt.figure(figsize=(20,10))
plt.bar(list(dict_score.keys()), list(dict_score.values()))
plt.xlabel('significance_score')
plt.ylabel('variables')
plt.xticks(rotation='vertical')
plt.savefig('C:\\Users\\tpatil\\Downloads\\score.png', dpi = 500)
plt.show()


# In[316]:


#dict_score.values()
import progressbar


# In[319]:


def permutation_features(x, y, model, baseline_metric):
    
    bar=progressbar.ProgressBar(len(X_test.columns))
    bar.start()
    baseline_metric = recall_score(Y_test, best_preds_test, average='macro')
    scores={c:[] for c in X_test.columns}
    for c in X_test.columns:
        print(c)
        for _ in range(10):
            x1= X_test.copy()
            print(type(x1))
            temp = x1[c].to_list()
            random.shuffle(temp)
            x1[c] = temp
            x1 = xgb.DMatrix(x1)
            predicted_score = model.predict(x1)
            predicted_score = np.asarray([np.argmax(line) for line in predicted_score])
            score = recall_score(Y_test, predicted_score, average='macro')
            scores[c].append(abs((baseline_metric - score)*100.00/baseline_metric))
        bar.update(x.columns.to_list().index(c))
    return scores    


# In[320]:


scores = permutation_features(X_test, Y_test, model, baseline_metric)


# In[321]:


scores


# In[328]:


import plotly_express as px
px.bar(pd.DataFrame.from_dict(scores).melt().groupby(['variable']).mean().reset_index().sort_values(['value'], ascending=False), x='variable', 
    y='value', labels={'variable':'column', 'value':'% change in recall'} )


# In[326]:


get_ipython().system('pip install plotly_express')


# In[332]:


pd.DataFrame.from_dict(scores).melt().groupby(['variable']).mean().reset_index().sort_values(['value'], ascending=False), x='variable', y='value', labels={'variable':'column', 'value':'% change in recall'}


# In[337]:


significance = pd.DataFrame.from_dict(scores).melt().groupby(['variable']).mean().reset_index().sort_values(['value'], ascending=False).iloc[1:]


# In[338]:


px.bar(significance,x='variable', y='value', labels={'variable':'column', 'value':'% change in recall'})


# In[341]:


df_data['is_level_1'].value_counts()


# In[ ]:




