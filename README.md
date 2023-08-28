# Youtube Channel Data Harvest
A Streamlit application that allows users to access and analyze data from multiple YouTube channels

# Skill Set
Python, Streamlit, MongoDB and MYSQL

# Approches

1. Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. Created a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data MongoDB.

2. Connect to the YouTube API: Genrated the API KEY to access the YouTube API and retrieve channel and video details. 

3. Store data in a MongoDB : Once retrieved the data from the YouTube API, stored it in a MongoDB with 3different collections(channel_data,video_data and comments_data).

4. Migrate data to a SQL data warehouse: After collected data for multiple channels, we can migrate it to a MYSQL DBfor querying the required data from it.

This project has 3 sections to implement the project scope.

    1.Data Collection
    2.Data Migration
    3.Data Analysis 

    Data Collection : To retreive the youtubse channel information(Channel basic informations, video details and comments of each video) and save it MongoDB.

    Data Migration : To migrate the youtube channel information from MongoDB to MYSQL.

    Data Analysis : To analyse the channels with follwoing basic 10 questions, based on each question we can see the analysis in either Table format orin Chart format.

    1. What are the names of all the videos and their corresponding channels?
    2. Which channels have the most number of videos, and how many videos do
    they have?
    3.What are the top 10 most viewed videos and their respective channels?
    4.How many comments were made on each video, and what are their
    corresponding video names?
    5. Which videos have the highest number of likes, and what are their 
    corresponding channel names?
    6. What is the total number of likes and dislikes for each video, and what are 
    their corresponding video names?
    7. What is the total number of views for each channel, and what are their 
    corresponding channel names?
    8. What are the names of all the channels that have published videos in the year
    2022?
    9. What is the average duration of all videos in each channel, and what are their 
    corresponding channel names?
    10. Which videos have the highest number of comments, and what are their 
    corresponding channel names?


# To Run the Project:
 streamlit run main.py 


# How to get Youtube channelID from Youtube?

1. Go to youtube channel home page
2. Right click -> View Page source
3. Search for "content="vnd.youtube://www.youtube.com/channel/" eg : content="vnd.youtube://www.youtube.com/channel/UCueYcgdqos0_PzNOq81zAFg"
4. ChannelID is "UCueYcgdqos0_PzNOq81zAFg"



# Processed Channel ids

I processed below channels during developement phase, 

1. Money Pechu : UC7fQFl37yAOaPaoxQm-TqSA => Having huge data
2. Parithapangal : UCueYcgdqos0_PzNOq81zAFg => Having huge data
3. Mano's Try : UCvtg5-HdnKexj8f54dqFC7g
4. Vijo's fitness & Life style : UC44HG3HMtxG99cSjufSTWqA
5. 1moRep : UCNmfEa6DKdYJMO31VG7UR_g
6. Excite Wealth in Tamil : UC0kTmNbPe4YJZjVmdXE6-yg
7. Fronendhub : UCqtoGNQhYHClb1VQcnW_yoQ
8. April Investment : UCBC0Qqu3J4EIKLHBl94UcxQ
9. Driving Zone : UCR24cSnmKfo1nGB8JTQyZiw
10. Code Nanban : UCyxny8lsaIIBN1EOi0fS8qQ
11. Robinson Shalu : UCjL9x3rFphQbGtiyntRqcgg
12. Wild Bettea : UCao_i5pRVAyxIgk0In0iVig
