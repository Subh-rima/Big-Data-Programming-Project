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


# In[2]:


import pymongo
import mongo
import json
from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import ReturnDocument
client = MongoClient('localhost',27017)
Cand = client['Candidate']
Details_collection = Cand['Selected'] 


# In[3]:


# Create Database
# In[4]:


import pandas as pd


# In[5]:



import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# In[6]:

if __name__ == "__main__":
    df = pd.read_csv(r'D:\Big Data Programming\GitHubDeskTop\big-data-programming-april2022-team-maas\csv_data\Data\github.csv')

# In[7]
# In[8]:


# df = df.rename(columns = {'git_user_email':'email'}, inplace = True)


# In[9]:


df1 = pd.DataFrame(df)
for col in df.columns:
    if 'last_upd_time' in col:
        del df[col] 
        


# In[10]:


df1 = pd.DataFrame(df)
for col in df.columns:
    if 'capture_time' in col:
        del df[col]


# In[11]:


# df1 = pd.DataFrame(df)
# for col in df.columns:
#     if 'git_user_email' in col:
#         del df[col]


# In[12]:


df1 = pd.DataFrame(df)
for col in df.columns:
    if 'git_repository' in col:
        del df[col]


# In[13]:


df1 = pd.DataFrame(df)
for col in df.columns:
    if 'git_created_at' in col:
        del df[col]


# In[14]:


df1 = pd.DataFrame(df)
for col in df.columns:
    if 'git_twitter_username' in col:
        del df[col]


# In[15]:


df


# In[16]:


df.isna().sum()


# In[17]:


df[" git_user_email"] = df[" git_user_email"].fillna('pihati7989@ploneix.com')


# In[18]:


df


# In[19]:


df = df.dropna()


# In[20]:


df.isna().sum()


# In[21]:


df


# In[22]:


features = df.select_dtypes(include=['int64', 'float', 'int'])
features.head() 


# In[23]:


from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression


# In[24]:


scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)


# In[25]:


kmeans = KMeans(init="random", n_clusters=2, n_init=10,
                max_iter=300, random_state = 47)
kmeans.fit(scaled_features)


# In[26]:


df['Selected'] = [True if label == 0 else False for label in kmeans.labels_]


# In[27]:


df


# In[28]:


# df.to_csv('out_file.csv',encoding="utf-8")
# 
# 
# In[33]:
# 
# 
# with pd.ExcelWriter('final_new.xlsx')as writer:
    #  df.to_excel(writer,sheet_name='Sheet_1')


# In[29]:


Y = df['Selected']
X = scaled_features


# In[30]:


X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.4)


# In[31]:


X_test, X_val, Y_text, Y_val = train_test_split(X_test, Y_test)


# # LOGISTIC REGRESSION

# In[32]:


model1 = LogisticRegression(solver='liblinear')
model1.fit(X_train, Y_train)
model1.score(X_train, Y_train)


# # SVM

# In[33]:


from sklearn.svm import SVC


# In[34]:


model2 = SVC(kernel='linear', random_state=0)
model2.fit(X_train, Y_train) 
model2.score(X_train, Y_train)


# In[35]:


Y = df['Selected']


# In[36]:


Y


# In[37]:


df_2 = df.loc[df['Selected'] == True]


# In[38]:


df['Selected'].value_counts()


# In[39]:


df_2


# In[40]:


# with pd.ExcelWriter('output_1.xlsx')as writer:
#     df.to_excel(writer,sheet_name='Sheet_1')


# In[41]:


# with pd.ExcelWriter('output_2.xlsx')as writer:
#     df_2.to_excel(writer,sheet_name='Sheet_1')


# In[42]:


df_2.to_csv('final_gm.csv',encoding="utf-8")


# In[43]:


import ssl
from email.message import EmailMessage
import smtplib


# In[47]:


# import pickle
# pickle.dump(model,open('email.pickle','wb'))


# In[48]:


# model_1 = pickle.load(open('email.pickle','rb'))


# In[60]:


client = MongoClient('localhost',27017)
Cand = client['Candidate']
Details_collection = Cand['Selected'] 
df_2.reset_index(inplace=True)
data_dict = df_2.to_dict("records")
# Insert collection
Details_collection.insert_many(data_dict)


# In[44]:


def read_records(collection, query=None):
    '''Returns the records matching the query parameter from the selected collection.
    If the query parameter is empty it will return all the records in the collection.'''
    return collection.find(query)


# In[45]:


candidates = [candidate for candidate in read_records(Details_collection)]
df_3 = pd.DataFrame(candidates)
# df_3 = df.set_index('_id')
df_3


# In[46]:


df_2


# In[47]:


df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if '_id' in col:
        del df_3[col] 


# In[48]:


df_3


# In[49]:


df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if 'index' in col:
        del df_3[col] 


# In[50]:


df_3


# In[51]:


df_3 = pd.DataFrame(df_3)
for col in df_3.columns:
    if 'level_0' in col:
        del df_3[col] 


# In[52]:


df_3


# In[53]:


def mails(df_3):
#     df = df.sort_values(by="weighted_score", ascending=False)
#     df = df.iloc[0:number_of_applicants, :]
     lst = df_3['email'].tolist()
     return lst

#sends email to email_list
def selected_email(lst):
    ADRESS="monicarksuresh@gmail.com"
    PASSWORD="nrnzszguexsccyrk"
    receiver=["dr.aparna1402@gmail.com","amruthgowd28@gmail.com","monica.rks@gmail.com"]

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

                Cheers,
                The-Hush-Hush-Recruiting Team"""

                msg=f'Subject:{subject}\n\n{body}'
                smtp.sendmail(ADRESS,i,msg)

selected_email(["dr.aparna1402@gmail.com","amruth.gowda31@gmail.com","monica.rks@gmail.com"])


# In[74]:


# def seleted_email(lst):
#     ADRESS="aparnardeshpande14299@gmail.com"
#     PASSWORD="xdoxabfdouwsrxfk"
#     receiver=["dr.aparna1402@gmail.com","amruth.gowda31@gmail.com","monicarksuresh@gmail.com"]

#     with smtplib.SMTP("smtp.gmail.com",587,timeout=100) as smtp:
#         smtp.ehlo()
#         smtp.starttls()
#         smtp.ehlo()
#         smtp.login(ADRESS,PASSWORD) 

#         for i in lst:
#             subject="Job Alert from hushHush Recruiter!"

#             body="""Congratulations! You've been selected for Doodle Foobar Challenge!
#             To participate in the challenge, please click on the link below:
#             https://www.surveymonkey.de/r/MCCDFR3
#             We wish you all the good luck!!
                
#             Cheers,
#             Team Doodle"""

#             msg=f'Subject:{subject}\n\n{body}'
#             smtp.sendmail(ADRESS,i,msg)

# selected_email(["dr.aparna1402@gmail.com","amruth.gowda31@gmail.com","monicarksuresh@gmail.com"])


# In[ ]:




