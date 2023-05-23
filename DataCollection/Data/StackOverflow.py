import mysql.connector
import pandas as pd
import requests
import datetime
from utils import db_scripts

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="admin",
  database="user_data_db"
)

def create_table(create_query):
    mycursor = mydb.cursor()
    mycursor.execute(create_query)

create_table(db_scripts.stack_create_stg)
create_table(db_scripts.stack_create_dm)
create_table(db_scripts.stack_create_users_dm)
create_table(db_scripts.stack_create_users_stg)
create_table(db_scripts.create_users_sp)

#monica_key: ZOgf72xosCV1XqdxgKxoAg((

payload = 'ZOgf72xosCV1XqdxgKxoAg(('
headers = {
   'KEY': 'ZOgf72xosCV1XqdxgKxoAg((',
   'api-key': 'ZOgf72xosCV1XqdxgKxoAg(('
}

#using the stackexchamge api to get user id and reputation
def fetching_data_stackoverflow(page: int):
    #Fetches user data from the stackexchange API
    users_url = f'https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&site=stackoverflow&pagesize=100&page={page}'
    response = requests.request("GET", users_url, headers=headers, data=payload)
    return response

def api_db_stackoverflow(no_of_pages_to_load):
    count=0
    flag=0
    for i in range(1,no_of_pages_to_load+1):
        #Print status
        print(f"Requesting page {i}/{no_of_pages_to_load}")

        #Get Data
        response = fetching_data_stackoverflow(i)
        result = response.json()

        for data in result['items']:

            try:
                str_tag=str(data['tags'])
                tag_value=((str_tag.replace("[","").replace("]","")).replace("'","")) #Tag Value
                try:
                    owner_reputation=data['owner']['reputation']
                except:
                    owner_reputation="NA"
                owner_user_id=data['owner']['user_id']
                owner_user_type=data['owner']['user_type']
                owner_display_name=data['owner']['display_name']
                try:
                    owner_link=data['owner']['link']
                except:
                    owner_link="NA"
                is_answered=data['is_answered']
                view_count=data['view_count']
                answer_count=data['answer_count']
                score=data['score']
                question_id=data['question_id']
                try:
                    content_license=data['content_license']
                except:
                    content_license="NA"
                title=data['title']
                link=data['link']
                last_activity_date=data['last_activity_date']
                email_id= data['owner']['display_name'] + str(data['owner']['user_id']) + "@gmail.com"
                capture_time=datetime.datetime.now()

                # mycursor_for_key = mydb.cursor()
                # mycursor_for_key.execute("""select owner_user_id from user_data_db.stackoverflow_data;""")
                # myresult = mycursor_for_key.fetchall()
                #if owner_user_id not in myresult:

                mycursor = mydb.cursor()

                sql = """INSERT INTO user_data_db.stg_stackoverflow_data
                (OWNER_USER_ID, OWNER_DISPLAY_NAME, TAGS, OWNER_REPUTATION, OWNER_USER_TYPE, OWNER_LINK, is_answered, view_count, answer_count, score, question_id, content_license, link, title, email_id, capture_time,last_activity_date) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

                val = (owner_user_id, owner_display_name, tag_value, owner_reputation, owner_user_type, owner_link, is_answered,view_count, answer_count, score, question_id, content_license, link, title, email_id, capture_time, last_activity_date)


                mycursor.execute(sql, val)
                mydb.commit()
                print(owner_user_id," is inserted")
                flag+=1


            except:
                count+=1

    print("invalid data ", count)
    print("valid data ", flag)


#api_db_stackoverflow(210)

#pushing non-duplicate data to stg
def stackoverflow_stg_dm():
    Query='''call user_data_db.STACKOVERFLOW_DATA_STG_to_DM();'''
    mycursor = mydb.cursor()
    mycursor.execute(Query)
    print(Query, 'executed', 'stg truncated')

#fetches user score, top tags
def user_api_to_db():
    Query='select OWNER_USER_ID from user_data_db.stg_stackoverflow_data;'
    mycursor = mydb.cursor()
    mycursor.execute(Query)
    result=mycursor.fetchall()

    user_id=[]

    for row in result:
        user_id.append(row[0])
    
    for i in range(0,len(user_id)):
        x = user_id[i]

        url = f"https://api.stackexchange.com/2.3/users/{x}/top-tags?pagesize=1&site=stackoverflow"
        response = requests.request("GET", url, headers=headers, data=payload)

        for x in response.json()['items']:
            user_id_col=x['user_id']
            answer_count=x['answer_count']
            answer_score=x['answer_score']
            question_count=x['question_count']
            question_score=x['question_score']
            tag_name=x['tag_name']

            sql = """INSERT INTO user_data_db.stg_stackoverflow_user_data (user_id,answer_count,answer_score,question_count,question_score,tag_name) VALUES (%s,%s,%s,%s,%s,%s)"""
            val=(user_id_col,answer_count,answer_score,question_count,question_score,tag_name)

            mycursor.execute(sql, val)
            mydb.commit()
            print((i, user_id_col, 'inserted'))

#checks for duplicates and pushes data to user_dm
def stackoverflow_user_stg_dm():
    Query='''call user_data_db.stg_dm_stackoverflow_user_data();'''
    mycursor = mydb.cursor()
    mycursor.execute(Query)
    print(Query, 'executed', 'stg truncated')

#reads data from mysql and converts to a dataframe/csv
def stack_dm_csv():
    Query='''SELECT data.OWNER_USER_ID,data.OWNER_DISPLAY_NAME,data.OWNER_REPUTATION, 
    user_data.answer_count,user_data.answer_score,user_data.question_count,user_data.question_score,user_data.tag_name, data.email_id
    FROM user_data_db.dm_stackoverflow_data data, user_data_db.dm_stackoverflow_user_data user_data
    where data.OWNER_USER_ID=user_data.user_id;'''
    mycursor = mydb.cursor()
    mycursor.execute(Query)
    result=mycursor.fetchall()

    USER_ID=[]
    DISPLAY_NAME=[]
    REPUTATION=[]
    answer_count=[]
    answer_score=[]
    question_count=[]
    question_score=[]
    tag_name=[]
    email_id = []

    for row in result:
        USER_ID.append(row[0])
        DISPLAY_NAME.append(row[1])
        REPUTATION.append(row[2])
        answer_count.append(row[3])
        answer_score.append(row[4])
        question_count.append(row[5])
        question_score.append(row[6])
        tag_name.append(row[7])
        email_id.append(row[8])

        dict_df={
        'USER_ID' : USER_ID,
        'DISPLAY_NAME' :  DISPLAY_NAME,
        'REPUTATION' :  REPUTATION,
        'answer_count' :  answer_count,
        'answer_score' :  answer_score,
        'question_count' :  question_count,
        'question_score' :  question_score,
        'tag_name' :  tag_name
    }
    
    df=pd.DataFrame(dict_df)
    #df_csv=df.to_csv("D:\Big Data Programming\GitHubDeskTop\big-data-programming-april2022-team-maas\CSV Data\stackoverflow.csv") 
    df_csv=df.to_csv("D:\Big Data Programming\Project\DataCollection\stack.csv", index=False) 
    print("data loaded into CSV")


api_db_stackoverflow(1500)
stackoverflow_stg_dm()
user_api_to_db()
stackoverflow_user_stg_dm()
stack_dm_csv()


