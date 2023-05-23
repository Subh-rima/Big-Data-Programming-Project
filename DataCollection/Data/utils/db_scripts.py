
stack_create_stg = '''CREATE TABLE IF NOT EXISTS user_data_db.stg_stackoverflow_data (
  `OWNER_USER_ID` varchar(100) DEFAULT NULL,
  `OWNER_DISPLAY_NAME` varchar(100) DEFAULT NULL,
  `TAGS` varchar(1000) DEFAULT NULL,
  `OWNER_REPUTATION` varchar(50) DEFAULT NULL,
  `OWNER_USER_TYPE` varchar(100) DEFAULT NULL,
  `OWNER_LINK` varchar(1000) DEFAULT NULL,
  `is_answered` varchar(50) DEFAULT NULL,
  `view_count` varchar(10) DEFAULT NULL,
  `answer_count` varchar(100) DEFAULT NULL,
  `score` varchar(100) DEFAULT NULL,
  `question_id` varchar(100) DEFAULT NULL,
  `content_license` varchar(100) DEFAULT NULL,
  `link` varchar(1000) DEFAULT NULL,
  `title` varchar(1000) DEFAULT NULL,
  `email_id` varchar(100) DEFAULT NULL,
  `capture_time` timestamp NULL DEFAULT NULL,
  `last_activity_date` varchar(100) DEFAULT NULL
);'''
stack_create_dm = '''CREATE TABLE IF NOT EXISTS user_data_db.dm_stackoverflow_data (
  `OWNER_USER_ID` varchar(100) NOT NULL,
  `OWNER_DISPLAY_NAME` varchar(100) DEFAULT NULL,
  `TAGS` varchar(1000) DEFAULT NULL,
  `OWNER_REPUTATION` varchar(50) DEFAULT NULL,
  `OWNER_USER_TYPE` varchar(100) DEFAULT NULL,
  `OWNER_LINK` varchar(1000) DEFAULT NULL,
  `is_answered` varchar(50) DEFAULT NULL,
  `view_count` varchar(10) DEFAULT NULL,
  `answer_count` varchar(100) DEFAULT NULL,
  `score` varchar(100) DEFAULT NULL,
  `question_id` varchar(100) DEFAULT NULL,
  `content_license` varchar(100) DEFAULT NULL,
  `link` varchar(1000) DEFAULT NULL,
  `title` varchar(1000) DEFAULT NULL,
  `email_id` varchar(100) DEFAULT NULL,
  `create_date` timestamp NULL DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`OWNER_USER_ID`)
)'''
stack_create_users_stg = '''create table IF NOT EXISTS user_data_db.stg_stackoverflow_user_data
(
user_id varchar(100),
answer_count varchar(100),
answer_score varchar(100),
question_count varchar(100),
question_score varchar(100),
tag_name varchar(200)
);'''
stack_create_users_dm = '''
create table IF NOT EXISTS user_data_db.dm_stackoverflow_user_data
(
user_id varchar(100),
answer_count varchar(100),
answer_score varchar(100),
question_count varchar(100),
question_score varchar(100),
tag_name varchar(200)
);
'''

create_users_sp = '''CREATE PROCEDURE `stg_dm_stackoverflow_user_data` ()
BEGIN
insert into user_data_db.dm_stackoverflow_user_data
select distinct * from user_data_db.stg_stackoverflow_user_data
where user_id not in (select user_id from user_data_db.dm_stackoverflow_user_data);

TRUNCATE TABLE user_data_db.stg_stackoverflow_user_data;
END'''

create_table_github_stg = '''CREATE TABLE `stg_github_data` (
  `GIT_USER_NAME` varchar(100) DEFAULT NULL,
  `GIT_LOGIN_NAME` varchar(100) DEFAULT NULL,
  `GIT_FOLLOWERS` varchar(100) DEFAULT NULL,
  `GIT_CREATED_BY` varchar(100) DEFAULT NULL,
  `GIT_TWITTER_USERNAME` varchar(100) DEFAULT NULL,
  `GIT_TOTAL_COUNT` varchar(100) DEFAULT NULL,
  `GIT_REPO_COUNT` varchar(100) DEFAULT NULL,
  `GIT_REPOSITORY` varchar(500) DEFAULT NULL,
  `GIT_USER_EMAIL` varchar(100) DEFAULT NULL
) '''

create_table_github_dm = '''create table if not exists USER_DATA_DB.DM_GITHUB_DATA
(
GIT_USER_NAME varchar(100),
GIT_LOGIN_NAME varchar(100),
GIT_FOLLOWERS varchar(100),
GIT_CREATED_at varchar(100),
GIT_TWITTER_USERNAME varchar(100),
GIT_TOTAL_COUNT varchar(100),
GIT_REPO_COUNT varchar(100),
GIT_REPOSITORY varchar(500),
GIT_USER_EMAIL varchar(100),
CAPTURE_TIME timestamp,
LAST_UPD_TIME timestamp
)'''

create_git_sp = '''CREATE DEFINER=`dev_user`@`localhost` PROCEDURE `GITHUB_DATA_STG_DM`()
BEGIN
UPDATE USER_DATA_DB.DM_GITHUB_DATA tgt
INNER JOIN user_data_db.stg_github_data stg ON tgt.GIT_LOGIN_NAME = stg.GIT_LOGIN_NAME
SET 
TGT.GIT_USER_NAME=STG.GIT_USER_NAME,
TGT.GIT_FOLLOWERS=STG.GIT_FOLLOWERS,
TGT.GIT_CREATED_AT=STG.GIT_CREATED_AT,
TGT.GIT_TWITTER_USERNAME=STG.GIT_TWITTER_USERNAME,
TGT.GIT_TOTAL_COUNT=STG.GIT_TOTAL_COUNT,
TGT.GIT_REPO_COUNT=STG.GIT_REPO_COUNT,
TGT.GIT_REPOSITORY=STG.GIT_REPOSITORY,
TGT.GIT_USER_EMAIL=STG.GIT_USER_EMAIL,
TGT.LAST_UPD_TIME=current_timestamp();

insert into USER_DATA_DB.DM_GITHUB_DATA
select distinct *, current_timestamp(),current_timestamp() from user_data_db.stg_github_data where (GIT_LOGIN_NAME,GIT_FOLLOWERS) in
(select GIT_LOGIN_NAME,max(GIT_FOLLOWERS) from user_data_db.stg_github_data a
group by 1)
and GIT_LOGIN_NAME not in (select GIT_LOGIN_NAME from USER_DATA_DB.DM_GITHUB_DATA);

TRUNCATE table user_data_db.stg_github_data;
END'''