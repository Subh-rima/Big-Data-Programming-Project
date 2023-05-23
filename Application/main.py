#!/usr/bin/env python
# coding: utf-8

# In[1]:


# pip install openpyxl


# In[2]:


# pip install pymongo


# In[4]:


# pip install mongo


# In[5]:


# pip install json


# In[115]:


import pymongo
import mongo
import json
from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import ReturnDocument
client = MongoClient('localhost',27017)
Cand = client['Candidates']
Details_collection = Cand['Selected'] 


# In[59]:


# Create Database
db = client["UsersData"]

def create_collection(collection_name: str):
    new_collection = db[collection_name]
    return new_collection


# In[72]:


import pandas as pd


# In[73]:



import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# In[74]:


df = pd.read_csv(r'D:\Big Data Programming\GitHubDeskTop\big-data-programming-april2022-team-maas\csv_data\Data\combined_final.csv')


# In[75]:


df


# In[76]:


# df = df.rename(columns = {'git_user_email':'email'}, inplace = True)


# In[77]:


df


# In[78]:


df.isna().sum()


# In[79]:


df["email"] = df["email"].fillna('pihati7989@ploneix.com')


# In[80]:


df


# In[81]:


def correlation(dataset, threshold):
    '''Feature selection using CM- Returns name of columns to drop due to high correlation with other features'''
    col_corr = set()
    corr_matrix = dataset.corr()
    for x in range(len(corr_matrix.columns)):
        for y in range(x):
            if corr_matrix.iloc[x,y] > threshold:     # Absolute Coefficient Value
                col_name = corr_matrix.columns[x]     # Get the name of the column
                col_corr.add(col_name)  
    return col_corr


# In[82]:


corr_features = correlation(df, 0.8)
# new_df = df1.drop(corr_features, axis=1)
len(set(corr_features))


# In[83]:


corr_features


# In[84]:


import matplotlib.pyplot as plt


# In[85]:


df.corr()


# In[86]:


import seaborn as sns


# In[87]:


ax,fig = plt.subplots(figsize=(18,9))
sns.heatmap(df.corr(),annot=True,cmap='YlGnBu')


# In[88]:


df = df.drop(corr_features, axis=1)


# In[89]:


df


# In[90]:


df = df.dropna()


# In[91]:


df.isna().sum()


# In[92]:


df


# In[93]:


features = df.select_dtypes(include=['int64', 'float', 'int'])
features.head() 
# features = features.drop(['answer_count','question_count'], axis=1)
# print(features)


# In[94]:


from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC


# In[95]:


features.describe()


# In[96]:


scaler = StandardScaler()
scaled = scaler.fit_transform(features)


# In[97]:


kmeans = KMeans(init="random", n_clusters=2, n_init=10,
                max_iter=300, random_state = 47)
kmeans.fit(scaled)


# In[98]:


df['Selected'] = [True if label == 1 else False for label in kmeans.labels_]


# In[99]:


df


# In[100]:


df.to_csv('Com_file.csv',encoding="utf-8")


# In[34]:


with pd.ExcelWriter('output_new.xlsx')as writer:
     df.to_excel(writer,sheet_name='Sheet_1')


# In[101]:


Y = df['Selected']
X = scaled


# In[102]:


X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.4)


# In[103]:


X_test, X_val, Y_text, Y_val = train_test_split(X_test, Y_test)


# In[104]:


model1 = LogisticRegression(solver='liblinear')
model1.fit(X_train, Y_train)
model1.score(X_train, Y_train) 


# In[105]:


Y = df['Selected']


# In[106]:


Y


# In[107]:


df_2 = df.loc[df['Selected'] == True]


# In[108]:


df['Selected'].value_counts()


# In[109]:


df_2


# In[41]:


# with pd.ExcelWriter('output_1.xlsx')as writer:
#     df.to_excel(writer,sheet_name='Sheet_1')


# In[42]:


# with pd.ExcelWriter('output_2.xlsx')as writer:
#     df_2.to_excel(writer,sheet_name='Sheet_1')


# In[110]:


df_2.to_csv('final_gs.csv',encoding="utf-8")


# In[111]:


import ssl
from email.message import EmailMessage
import smtplib


# In[114]:


client = MongoClient('localhost',27017)
Cand = client['Selected_candidate']
Details_collection = Cand['Selected'] 
df_2.reset_index(inplace=True)
data_dict = df_2.to_dict("records")
# Insert collection
Details_collection.insert_many(data_dict)


# In[116]:


def read_records(collection, query=None):
    '''Returns the records matching the query parameter from the selected collection.
    If the query parameter is empty it will return all the records in the collection.'''
    return collection.find(query)


# In[117]:


Selected_candidates = [candidate for candidate in read_records(Details_collection)]
df_3 = pd.DataFrame(Selected_candidates)
# df_3 = df.set_index('_id')
df_3


# In[118]:


df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if '_id' in col:
        del df_3[col] 
        
df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if 'index' in col:
        del df_3[col] 
        
df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if 'level_0' in col:
        del df_3[col] 
        
df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if 'git_fuzzy_name' in col:
        del df_3[col] 


# In[119]:


df_3


# In[120]:


def mails(df_3):
#     df = df.sort_values(by="weighted_score", ascending=False)
#     df = df.iloc[0:number_of_applicants, :]
     lst = df_3['email'].tolist()
     return lst

# print(mails(df_3))

#sends email to email_list
def selected_email(lst):
    ADRESS="amruth.gowda31@gmail.com"
    PASSWORD="pbtdufufedknkzcq"
    receiver=["dr.aparna1402@gmail.com","amruthgowd28@gmail.com","monica.rks@gmail.com","subhrimalodh@gmail.com"]

    with smtplib.SMTP("smtp.gmail.com",587,timeout=100) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(ADRESS,PASSWORD)

        for i in lst:
           
           
                subject="Job Alert from hushHush Recruiter!"

                body="""Congratulations! You've been selected for Doodle Foobar Challenge!
                To participate in the challenge, please click on the link below:
                https://forms.pabbly.com/form/share/o9ys-493000
                We wish you all the good luck!!

                Cheers,
                Team Doodle"""

                msg=f'Subject:{subject}\n\n{body}'
                smtp.sendmail(ADRESS,i,msg)

selected_email(["dr.aparna1402@gmail.com","amruth.gowda31@gmail.com","monica.rks@gmail.com","subhrimalodh@gmail.com"])


# In[ ]:




