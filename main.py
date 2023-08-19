import streamlit as st
from googleapiclient.discovery import build
import pymongo
from streamlit_option_menu import option_menu
import streamlit.components.v1 as html
from  PIL import Image
import mysql.connector 
from mysql.connector import Error
import pandas as pd
import plotly.express as px

#streamlit run /home/quest/GVA/pythonProj/Projects/Utube-data-harvest/main.py

#Ex : content="vnd.youtube://www.youtube.com/channel/UCueYcgdqos0_PzNOq81zAFg"
# quvi-utube-harvest-2

# Processed Channel ids
#Money Pechu : UC7fQFl37yAOaPaoxQm-TqSA
#Parithapangal : UCueYcgdqos0_PzNOq81zAFg
#Mano's Try : UCvtg5-HdnKexj8f54dqFC7g
#Vijo's fitness & Life style : UC44HG3HMtxG99cSjufSTWqA
#1moRep : UCNmfEa6DKdYJMO31VG7UR_g
#Excite Wealth in Tamil : UC0kTmNbPe4YJZjVmdXE6-yg
#Fronendhub : UCqtoGNQhYHClb1VQcnW_yoQ
#April Investment : UCBC0Qqu3J4EIKLHBl94UcxQ
#Driving Zone : UCR24cSnmKfo1nGB8JTQyZiw
#Code Nanban : UCyxny8lsaIIBN1EOi0fS8qQ

#Robinson Shalu : UCjL9x3rFphQbGtiyntRqcgg

st.title('U-Tube Channel data Harvest')

with st.sidebar:
    #st.sidebar.title('Analysis')
    choose = option_menu("Data Harvest", ["Collection", "Migration", "Analysis"],
                         icons=['collection', 'database-up', 'graph-up'],
                         menu_icon="activity", default_index=0,
                         styles={
                            "container": {"padding": "5!important", "background-color": "#fafafa"},
                            "icon": {"color": "orange", "font-size": "25px"}, 
                            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                            "nav-link-selected": {"background-color": "#1482e3"},
                        }
    )


#API KEY : AIzaSyBRFakMlxJW7rSFNf-wYsRAtleaMducUqk
API_KEY = 'AIzaSyBBiolyaMjDDssDBYEoFVoO9t1_A5kcZAA'
youtube = build('youtube','v3',developerKey=API_KEY)

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['utube_channel']


connection = mysql.connector.connect(
        host ="localhost",
        user = "root",
        password = "@Jeeva.Arul212",
        #database= "utube_channel"
)
cursor = connection.cursor()

#Create DB
cursor.execute("CREATE DATABASE IF NOT EXISTS utube_channel")

#Create Table id int(16) NOT NULL AUTO_INCREMENT,
createChannelTable = """CREATE TABLE IF NOT EXISTS utube_channel.channel ( 
                             channel_id varchar(100) NOT NULL,
                             name varchar(255) NOT NULL,
                             subs_count BIGINT NOT NULL,
                             view_count BIGINT NOT NULL,
                             video_count int DEFAULT 0,
                             description LONGTEXT NOT NULL,
                             country varchar(100) DEFAULT 'IN',
                             PRIMARY KEY (channel_id)) """
cursor.execute(createChannelTable)

#Create Table id int(16) NOT NULL AUTO_INCREMENT,
createVideoTable = """CREATE TABLE IF NOT EXISTS utube_channel.videos ( 
                             channel_name varchar(200) NOT NULL,
                             channel_id varchar(100) NOT NULL,
                             video_id varchar(100) NOT NULL,
                             title varchar(200) NOT NULL,
                             thumbnail varchar(100) NOT NULL,
                             description LONGTEXT NOT NULL,
                             published_date varchar(100) NOT NULL,
                             duration varchar(200) NOT NULL,
                             view_count BIGINT DEFAULT 0,
                             like_count BIGINT DEFAULT 0,
                             comment_count int DEFAULT 0,
                             favorite_count int DEFAULT 0,
                             caption_status varchar(50) NOT NULL,
                             PRIMARY KEY (video_id)) """
cursor.execute(createVideoTable)

#Create Table id int(16) NOT NULL AUTO_INCREMENT,
createCommentTable = """CREATE TABLE IF NOT EXISTS utube_channel.comments ( 
                             comment_id varchar(100) NOT NULL,
                             video_id varchar(100) NOT NULL,
                             channel_id varchar(100) NOT NULL,
                             comment LONGTEXT NOT NULL,
                             author varchar(255) NOT NULL,
                             posted_date varchar(100) NOT NULL,
                             like_count BIGINT DEFAULT 0,
                             reply_count BIGINT DEFAULT 0,
                             PRIMARY KEY (comment_id)) """
cursor.execute(createCommentTable)



def getChannelData(channelId):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channelId).execute()
    #print(response)
    for i in range(len(response['items'])):
        snippet = response['items'][i]['snippet']
        stats = response['items'][i]['statistics']
        data = dict(channel_id = channelId,
            name = snippet['title'],
            #Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            #Published_At = response['items'][i]['snippet']['publishedAt'],
            subs_count = int(stats['subscriberCount']),
            view_count = int(stats['viewCount']),
            total_video = int(stats['videoCount']),
            details = snippet['description'],
            country = snippet.get('country'))
        ch_data.append(data)
    return ch_data


def getVideoList(channelId):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channelId, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


def getVideoDetails(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            #print(video)
            video_details = dict(channel_name = video['snippet']['channelTitle'],
                                channel_id = video['snippet']['channelId'],
                                video_id = video['id'],
                                title = video['snippet']['title'],
                                tags = video['snippet'].get('tags'),
                                thumbnail = video['snippet']['thumbnails']['default']['url'],
                                description = video['snippet']['description'],
                                published_date = video['snippet']['publishedAt'],
                                duration = video['contentDetails']['duration'],
                                view_count = int(video['statistics']['viewCount']),
                                like_count = video['statistics'].get('likeCount'),
                                comment_count = video['statistics'].get('commentCount'),
                                favorite_count = int(video['statistics']['favoriteCount']),
                                definition = video['contentDetails']['definition'],
                                caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats

def getCommentsDetails(v_id, channelId):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                snippet = cmt['snippet']['topLevelComment']['snippet']

                data = dict(comment_id = cmt['id'],
                    video_id = cmt['snippet']['videoId'],
                    channel_id = channelId,
                    comment = snippet['textDisplay'],
                    author = snippet['authorDisplayName'],
                    posted_date = snippet['publishedAt'],
                    like_count = snippet['likeCount'],
                    reply_count = cmt['snippet']['totalReplyCount'])
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

def getChannelList():   
    ch_name = []
    for channelObj in db.channel_data.find():
        data = dict(name = channelObj['name'], channel_id = channelObj['channel_id'])
        ch_name.append(data)
    return ch_name

if choose == "Collection":
    title = st.text_input('Please enter Youtube channel name')
    st.write('The Channel name is', title)

    if title and st.button("Fetch Data"):
                ch_details = getChannelData(title)
                #print(ch_details)
                st.write(f'#### Extracted data from :green["{ch_details[0]["name"]}"] channel')
                st.table(ch_details)

    if title and st.button("Upload to MongoDB"):
                with st.spinner('Please Wait for it...'):
                    ch_details = getChannelData(title)
                    videoIdLst = getVideoList(title)
                    videoLst = getVideoDetails(videoIdLst)
                    
                    def comments():
                        com_d = []
                        for vId in videoIdLst:
                            com_d+= getCommentsDetails(vId, title)
                        return com_d
                    commentList = comments()

                    col_channel = db.channel_data
                    col_channel.insert_many(ch_details)

                    col_video = db.video_data
                    col_video.insert_many(videoLst)

                    col_comments = db.comments_data
                    col_comments.insert_many(commentList)
                    st.success("Upload to MogoDB successful !!")

elif choose == "Migration":
    st.header('Migration')
    st.markdown("#   ")
    st.markdown("### Select a channel to begin Transformation to SQL")
        
    channelLst = getChannelList()

    channelNames = []
    channelIds = []
    for dictionary in channelLst:
        if 'name' in dictionary:
            channelNames.append(dictionary['name'])
        if 'channel_id' in dictionary:
            channelIds.append(dictionary['channel_id'])

    user_inp = st.selectbox("Select channel",options= channelNames)

    selectedIndex = channelNames.index(user_inp)
    choosenChannelId = channelIds[selectedIndex]

    print(choosenChannelId)

    #st.markdown("#   ")
    #st.markdown("### Selected channel is", str(choosenChannelId))
    def insert_into_channels():
                col_channel = db.channel_data
                #(%d,%s,%s,%d,%d,%d,%s,%s)
                query = """INSERT INTO utube_channel.channel VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                for i in col_channel.find({"channel_id" : choosenChannelId},{'_id':0}):
                    #print(query,tuple(i.values()))
                    cursor.execute(query,tuple(i.values()))
                    connection.commit()
                
    def insert_into_videos():
        col_video = db.video_data
        query1 = """INSERT INTO utube_channel.videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in col_video.find({"channel_id" : choosenChannelId},{"_id":0, "tags" : 0, "definition" : 0}):
            t=tuple(i.values())
            #print(t)
            cursor.execute(query1,t)
            connection.commit()

    def insert_into_comments():
        col_video = db.video_data
        col_comment = db.comments_data
        query2 = """INSERT INTO utube_channel.comments VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

        for vid in col_video.find({"channel_id" : choosenChannelId},{'_id' : 0}):
            for i in col_comment.find({'video_id': vid['video_id']},{'_id' : 0}):
                
                t=tuple(i.values())
                #print(t)
                cursor.execute(query2,t)
                connection.commit()

    if st.button("Submit"):
        try:
            
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            st.success("Migration of selected collection to MySQL successful")
        except mysql.connector.Error as error:
            st.error("Failed to insert record into table {}".format(error))
            #st.error("Channel details already transformed!!")



elif choose == "Analysis":
     st.header('Analysis')
     st.write("## :orange[Select any question to get Insights]")
     questionLst = ['Click the question that you would like to query',
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
     selectedQues = st.selectbox('Questions',questionLst, index=1)
     
     if questionLst.index(selectedQues) == 1 :
        cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM utube_channel.videos ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
     elif questionLst.index(selectedQues) == 2:
        cursor.execute("""SELECT name 
        AS Channel_Name, video_count AS Total_Videos
                            FROM utube_channel.channel
                            ORDER BY video_count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= cursor.column_names[0],y= cursor.column_names[1])
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
     elif questionLst.index(selectedQues) == 3:
        cursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, view_count AS Views 
                            FROM utube_channel.videos
                            ORDER BY view_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
     elif questionLst.index(selectedQues) == 4:
        cursor.execute("""SELECT a.channel_name AS Channel, a.title AS Video_Title, b.Total_Comments
                            FROM utube_channel.videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM utube_channel.comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[0],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
          
     elif questionLst.index(selectedQues) == 5:
        cursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,like_count AS Likes_Count 
                            FROM utube_channel.videos
                            ORDER BY like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
     elif questionLst.index(selectedQues) == 6:
        cursor.execute("""SELECT title AS Title, like_count AS Likes_Count
                            FROM utube_channel.videos
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
         
     elif questionLst.index(selectedQues) == 7:
        cursor.execute("""SELECT name AS Channel_Name, view_count AS Views
                            FROM utube_channel.channel
                            ORDER BY view_count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
     elif questionLst.index(selectedQues) == 8:
        cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM utube_channel.videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
     elif questionLst.index(selectedQues) == 9:
        cursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM utube_channel.videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names
                          )
        st.write(df)
        st.write("### :green[Average video duration for channels :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

        
     elif questionLst.index(selectedQues) == 10:
        cursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comment_count AS Comments
                            FROM utube_channel.videos
                            ORDER BY comment_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=cursor.column_names[1],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
