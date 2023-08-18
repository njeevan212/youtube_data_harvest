import streamlit as st
from googleapiclient.discovery import build
import pymongo



#streamlit run /home/quest/GVA/pythonProj/Projects/Utube-data-harvest/main.py

#Ex : content="vnd.youtube://www.youtube.com/channel/UCueYcgdqos0_PzNOq81zAFg"

st.title('U-Tube Channel data Harvest')


#API KEY : AIzaSyBRFakMlxJW7rSFNf-wYsRAtleaMducUqk
API_KEY = 'AIzaSyBRFakMlxJW7rSFNf-wYsRAtleaMducUqk'
youtube = build('youtube','v3',developerKey=API_KEY)

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['utube_channel']

def getChannelData(channelId):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channelId).execute()
    #print(response)
    for i in range(len(response['items'])):
        data = dict(channel_id = channelId,
                    name = response['items'][i]['snippet']['title'],
                    #Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    #Published_At = response['items'][i]['snippet']['publishedAt'],
                    subs_count = int(response['items'][i]['statistics']['subscriberCount']),
                    view_count = int(response['items'][i]['statistics']['viewCount']),
                    total_video = int(response['items'][i]['statistics']['videoCount']),
                    details = response['items'][i]['snippet']['description'],
                    country = response['items'][i]['snippet'].get('country')
                    )
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

def getCommentsDetails(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(comment_id = cmt['id'],
                            video_id = cmt['snippet']['videoId'],
                            comment = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

title = st.text_input('Please enter Youtube channel name')
st.write('The Channel name is', title)

#Example
#Money Pechu : UC7fQFl37yAOaPaoxQm-TqSA
#Parithapangal : UCueYcgdqos0_PzNOq81zAFg

if title and st.button("Fetch Data"):
            ch_details = getChannelData(title)
            #print(ch_details)
            st.write(f'#### Extracted data from :green["{ch_details[0]["name"]}"] channel')
            st.table(ch_details)

if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = getChannelData(title)
                videoIdLst = getVideoList(title)
                videoLst = getVideoDetails(videoIdLst)
                
                def comments():
                    com_d = []
                    for vId in videoIdLst:
                        com_d+= getCommentsDetails(vId)
                    return com_d
                commentList = comments()

                col_channel = db.channel_data
                col_channel.insert_many(ch_details)

                col_video = db.video_data
                col_video.insert_many(videoLst)

                col_comments = db.comments_data
                col_comments.insert_many(commentList)
                st.success("Upload to MogoDB successful !!")