from github import Github
from utils import db_scripts
import mysql.connector
import pandas as pd


#monica_token: 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
github_token='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'
g=Github(github_token)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="admin",
  database="user_data_db"
)

def create_table(create_query):
    mycursor = mydb.cursor()
    mycursor.execute(create_query)

#creating the staging, datamart and stored proc to move non-duplicate data from stg to dm
create_table(db_scripts.create_table_github_stg)
create_table(db_scripts.create_table_github_dm)
create_table(db_scripts.create_git_sp)


#gets data from api to database
def git_db_stg(no_of_records_to_pull):
    count=0
    #insert_flag=0
    for i in g.get_users()[0 :no_of_records_to_pull+1]:
        try:
            if i.login!=None and i.login!='':
                #insert_flag=insert_flag+1
                git_user_name=i.name
                git_login_name=str(i.login)
                git_followers=i.followers
                git_created_at=i.created_at
                git_twitter_username=i.twitter_username
                git_total_count=str(i.get_starred().totalCount)
                git_repo_count=str(i.get_repos().totalCount)
                git_repository=str(i.get_repos()[0]) #reference)
                git_user_email=i.email

                mycursor = mydb.cursor()

                sql = """INSERT INTO user_data_db.stg_github_data 
                    (GIT_USER_NAME, GIT_LOGIN_NAME, GIT_FOLLOWERS, GIT_CREATED_at, GIT_TWITTER_USERNAME, GIT_TOTAL_COUNT, GIT_REPO_COUNT, GIT_REPOSITORY, GIT_USER_EMAIL) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

                val = (git_user_name, git_login_name, git_followers, git_created_at, git_twitter_username, git_total_count, git_repo_count, git_repository, git_user_email) 

                mycursor.execute(sql, val)
                mydb.commit()
                #print((insert_flag, i.name, 'inserted'))

                #if insert_flag==no_of_records_to_pull:
                    #print('flag count reached terminating insert')
                    #break

        except:
            count+=1

    print(count,"invalid no. of data")

#storing the final data in the github_dm table removing duplicates
def git_stg_dm():
    Query='call user_data_db.GITHUB_DATA_STG_DM();'

    mycursor = mydb.cursor()
    mycursor.execute(Query)
    print(Query, 'executed', 'stg truncated')

#reading the raw data from MySql and converting to a dataframe/csv  
def dm_csv():
    Query='select * from USER_DATA_DB.DM_GITHUB_DATA;'

    mycursor = mydb.cursor()
    mycursor.execute(Query)
    result=mycursor.fetchall()

    git_user_name=[]
    git_login_name=[]
    git_followers=[]
    git_created_at=[]
    git_twitter_username=[]
    git_total_count=[]
    git_repo_count=[]
    git_repository=[]
    git_user_email=[]
    capture_time=[]
    last_upd_time=[]

    for row in result:
        git_user_name.append(row[0])
        git_login_name.append(row[1])
        git_followers.append(row[2])
        git_created_at.append(row[3])
        git_twitter_username.append(row[4])
        git_total_count.append(row[5])
        git_repo_count.append(row[6])
        git_repository.append(row[7])
        git_user_email.append(row[8])
        capture_time.append(row[9])
        last_upd_time.append(row[10])

    dict_df={'git_user_name' : git_user_name,
    ' git_login_name' :  git_login_name,
    ' git_followers' :  git_followers,
    ' git_created_at' :  git_created_at,
    ' git_twitter_username' :  git_twitter_username,
    ' git_total_count' :  git_total_count,
    ' git_repo_count' :  git_repo_count,
    ' git_repository' :  git_repository,
    ' git_user_email' :  git_user_email,
    ' capture_time' :  capture_time,
    ' last_upd_time' :  last_upd_time
    }
    df=pd.DataFrame(dict_df)
    #df_csv=df.to_csv("D:\Big Data Programming\Project\DataCollection\github.csv", index=False)
    df_csv=df.to_csv("D:\Big Data Programming\Project\DataCollection\github.csv", encoding = "utf-8")   
    print("data loaded into CSV")
    #df.to_csv("file.csv", encoding="utf-8") 
    ##!/usr/bin/python
    # -*- coding: latin-1 -*-


git_db_stg(18000) 
git_stg_dm()
dm_csv()





