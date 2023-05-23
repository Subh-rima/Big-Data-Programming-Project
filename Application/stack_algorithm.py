#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# In[2]:


df= pd.read_csv(r'D:\Big Data Programming\GitHubDeskTop\big-data-programming-april2022-team-maas\csv_data\Data\stack_gmail.csv')


# In[3]:


df


# In[4]:


df.head()


# In[5]:


df.info()


# In[6]:


df.isna().sum()


# In[7]:


df["tag_name"] = df["tag_name"].fillna('python')


# In[8]:


df.isna().sum()


# In[9]:


from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression


# In[10]:


df


# In[11]:


df=df.drop(["answer_count","question_count","question_score"],axis=1)


# In[12]:


df


# In[13]:


df1 = df.select_dtypes(include=['int64', 'float', 'int'])


# 

# In[14]:


df1


# In[15]:


df1=df1.drop("USER_ID",axis=1)


# In[16]:


df1


# In[17]:


from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression


# In[18]:


scaler = StandardScaler()
scaled_features = scaler.fit_transform(df1)


# In[19]:


kmeans = KMeans(init="random", n_clusters=2, n_init=10,
                max_iter=300, random_state = 47)
kmeans.fit(scaled_features)


# In[20]:


df['Selected'] = [True if label == 0 else False for label in kmeans.labels_]


# In[21]:


Y = df['Selected']
X = scaled_features


# In[22]:


Y


# In[23]:


X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.4)


# In[24]:


X_test, X_val, Y_text, Y_val = train_test_split(X_test, Y_test)


# In[ ]:





# # Logistic regression

# In[25]:


model = LogisticRegression(solver='liblinear')
model.fit(X_train, Y_train)
model.score(X_train, Y_train) 


# # SVM

# In[26]:


from sklearn.svm import SVC
model2 = SVC(kernel='linear', random_state=0)
model2.fit(X_train, Y_train) 
model2.score(X_train, Y_train)


# In[28]:


df["email"] = df["email"].fillna('pihati7989@ploneix.com')


# In[29]:


df


# In[33]:


df_2 = df.loc[df['Selected'] == True]


# In[34]:


df['Selected'].value_counts()


# In[35]:


df_2


# In[36]:


df_2.to_csv("stackgmailtrue.csv")


# In[38]:


import ssl
from email.message import EmailMessage
import smtplib


# In[40]:


def mails(df_2):
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
            body="""Congratulations! You've been selected for the Hush Hush recruiter Challenge!
            To participate in the challenge, please click on the link below:
            https://forms.pabbly.com/form/share/o9ys-493000
            We wish you all the good luck!!

            Cheers,
            The-Hush-Hush-Recruiting Team"""

            msg=f'Subject:{subject}\n\n{body}'
            smtp.sendmail(ADRESS,i,msg)

selected_email(["dr.aparna1402@gmail.com","amruth.gowda31@gmail.com","monica.rks@gmail.com"])


# In[ ]:




