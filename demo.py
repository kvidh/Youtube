from googleapiclient.discovery import build
from pprint import pprint
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st


def Api_connect():
    Api_id="AIzaSyDhoIpdtlz-brvke3Fo9a-CG1txVqex3YY"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    
    return youtube

youtube=Api_connect()

def convert_dur(s):
  l = []
  f = ''
  for i in s:
    if i.isnumeric():
      f = f+i
    else:
      if f:
        l.append(f)
        f = ''
  if 'H' not in s:
    l.insert(0,'00')
  if 'M' not in s:
    l.insert(1,'00')
  if 'S' not in s:
    l.insert(-1,'00')
  return ':'.join(l)

#channel infos
def get_channel_info(channel_id):
        request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
        response = request.execute()

       
        for i in response['items']:
                data=dict(Channel_Name=i["snippet"]["title"],
                        Channel_Id=i["id"],
                        Subscribers=i["statistics"]["subscriberCount"],
                        Views=i["statistics"]["viewCount"],
                        Total_Videos=i["statistics"]["videoCount"],
                        Channel_Description=i["snippet"]["description"],
                        Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data


#3.get video Ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids

#4.video idoda specific details- get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=",".join(item['snippet'].get('tags',['na'])),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=convert_dur(item['contentDetails']['duration']),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

#getcomment details info
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet'][ 'authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:#fr comment disabled videos
        pass
    return Comment_data

#upload to mongo db
client=pymongo.MongoClient("mongodb+srv://kvidhya:1234@cluster0.cipnxl7.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload success"

st.title(":rainbow[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
c_id=st.text_input("Enter Channel Id")

if c_id and st.button("Go"):
     x=channel_details(c_id)
     st.write(x)

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="youtube_data",
        port="3306")
print(mydb)
mycursor = mydb.cursor(buffered=True)

def channel(c_id):
    
    mycursor.execute("CREATE TABLE IF NOT EXISTS Channels(Channel_Name VARCHAR(100),Channel_Id VARCHAR(80) primary key,Subscribers BIGINT,Views BIGINT,Total_Videos BIGINT,Channel_Description TEXT,Playlist_Id VARCHAR(80))")
    mycursor.execute("CREATE TABLE IF NOT EXISTS Videos(Channel_Name VARCHAR(100),Channel_Id VARCHAR(100),Video_Id VARCHAR(100) PRIMARY KEY,Title VARCHAR(200) ,Tags TEXT NULL,Thumbnail VARCHAR(50),Description TEXT NULL,Published_Date TIMESTAMP,Duration TIME,Views BIGINT NULL,Likes BIGINT NULL,Comments INT(50) NULL,Favorite_Count INT(50),Definition VARCHAR(10),Caption_Status VARCHAR(80))")
    mycursor.execute("CREATE TABLE IF NOT EXISTS Comments(Comment_Id VARCHAR(100),Video_Id VARCHAR(80),Comment_Text VARCHAR(200),Comment_Author VARCHAR(100),Comment_Published TIMESTAMP)")

    db=client["Youtube_data"]
    coll=db["channel_details"]

    data=coll.find_one({"channel_information.Channel_Id":c_id})

    sql='''INSERT INTO channels(Channel_Name,Channel_Id,Subscribers,Views,Total_Videos,Channel_Description,Playlist_Id) VALUES(%s,%s,%s,%s,%s,%s,%s)'''
    val=tuple(data["channel_information"].values())
    mycursor.execute(sql,val)
    mydb.commit()

    sql1='''INSERT INTO videos(Channel_Name,Channel_Id,Video_Id,Title,Tags,Thumbnail,Description,Published_Date,Duration,Views,Likes,Comments,Favorite_Count,Definition,Caption_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for i in data["video_information"]:
        val1=tuple(i.values())
        mycursor.execute(sql1,val1)
        mydb.commit()

    sql2='''INSERT INTO Comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published) VALUES (%s,%s,%s,%s,%s)'''
    for i in data["comment_information"]:
        val2=tuple(i.values())
        mycursor.execute(sql2,val2)
        mydb.commit()
        return "Values inserted in tables"


if st.button("MOVE TO SQL"):
    y=channel(c_id)
    st.write(y)

Query_Output=st.selectbox("Select Question",
                          ("1.Videos with corresponding Channel Name",
                           "2.which channel have most number of videos and how many?",
                           "3.top 10 most viewed videos?",
                           "4.how many comments in each video along with video name?",
                           "5.Video with max no of likes along with channel names",
                           "6.Video with likes count along with channel names",
                           "7.Total Number of views for each channel along wit channel names",
                           "8.channel names that published videos in the year 2022",
                           "9.Average duration of all videos in each channel along with channel names:",
                           "10.which videos have highest number of comments along with channel names?"))

if Query_Output=="1.Videos with corresponding Channel Name":
        query1='''SELECT Title,Channel_Name FROM videos'''
        mycursor.execute(query1)
        mydb.commit()
        t1=mycursor.fetchall()
        df1=pd.DataFrame(t1,columns=["Video","ChannelName"])
        st.write(df1)

elif Query_Output=="2.which channel have most number of videos and how many?":
        query2='''SELECT Channel_Name, COUNT(Video_Id) AS Video_Count FROM videos GROUP BY Channel_Name ORDER BY Video_Count DESC LIMIT 1'''
        mycursor.execute(query2)
        mydb.commit()
        t2=mycursor.fetchall()
        df2=pd.DataFrame(t2,columns=["Channel_Name ","Video_Count"])
        st.write(df2)

elif Query_Output=="3.top 10 most viewed videos?":
        query3='''SELECT Channel_Name,Title, Views FROM videos ORDER BY Views DESC LIMIT 10'''
        mycursor.execute(query3)
        mydb.commit()
        t3=mycursor.fetchall()
        df3=pd.DataFrame(t3,columns=["Channel_Name ","Title ","Views "])
        st.write(df3)

elif Query_Output=="4.how many comments in each video along with video name?":
        query4='''SELECT Video_Id, Title AS Video_Name, Comments FROM videos'''
        mycursor.execute(query4)
        mydb.commit()
        t4=mycursor.fetchall()
        df4=pd.DataFrame(t4,columns=["Video_Id","Video_Name"," Comments"])
        st.write(df4)

elif Query_Output=="5.Video with max no of likes along with channel names":
        query5='''SELECT Title,Likes,Channel_Name FROM videos ORDER BY Likes DESC LIMIT 20'''
        mycursor.execute(query5)
        mydb.commit()
        t5=mycursor.fetchall()
        df5=pd.DataFrame(t5,columns=["Title","Likes","Channel_Name"])
        st.write(df5)

elif Query_Output=="6.Video with likes count along with channel names":
        query6='''SELECT Title,Likes,Channel_Name FROM videos'''
        mycursor.execute(query6)
        mydb.commit()
        t6=mycursor.fetchall()
        df6=pd.DataFrame(t6,columns=["Title","Likes"," Channel_Name"])
        st.write(df6)

elif Query_Output=="7.Total Number of views for each channel along wit channel names":
        query7='''SELECT Channel_Name,Views FROM channels'''
        mycursor.execute(query7)
        mydb.commit()
        t7=mycursor.fetchall()
        df7=pd.DataFrame(t7,columns=["Channel_Name","Views"])
        st.write(df7)

elif Query_Output=="8.channel names that published videos in the year 2022":
        query8='''SELECT DISTINCT Channel_Name FROM videos WHERE YEAR(Published_Date) = 2022'''
        mycursor.execute(query8)
        mydb.commit()
        t8=mycursor.fetchall()
        df8=pd.DataFrame(t8,columns=["Channel_Name "])
        st.write(df8)

elif Query_Output=="9.Average duration of all videos in each channel along with channel names:":
        query9='''SELECT Channel_Name, AVG(Duration) FROM videos GROUP BY Channel_Name'''
        mycursor.execute(query9)
        mydb.commit()
        t9=mycursor.fetchall()
        df9=pd.DataFrame(t9,columns=["Channel_Name","AVG(Duration)"])
        st.write(df9)

elif Query_Output=="10.which videos have highest number of comments along with channel names?":
        query10='''SELECT Title, Channel_Name, Comments FROM videos ORDER BY Views DESC LIMIT 20'''
        mycursor.execute(query10)
        mydb.commit()
        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Title ","Channel_Name","Comment_Count"])
        st.write(df10)

