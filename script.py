import json
from googleapiclient.discovery import build
from hdfs import InsecureClient

YOUTUBE_API_KEY = 'AIzaSyA9FM862DP7rtVRjYW1wsYLBVrzc8XuW-k'
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
client = InsecureClient('http://localhost:9870', user='root')

def fetch_comprehensive_data(query):
    try:
        print(f" Collecting Data for: {query}...")
        
        all_video_data = []
        search_req = youtube.search().list(q=query, part='snippet', maxResults=50, type='video')
        search_res = search_req.execute()

        for item in search_res.get('items', []):
            v_id = item['id']['videoId']
            
  
            stats_req = youtube.videos().list(part="statistics", id=v_id)
            stats_res = stats_req.execute()
            stats = stats_res['items'][0]['statistics'] if stats_res['items'] else {}


            comments = []
            try:
                comment_req = youtube.commentThreads().list(part="snippet", videoId=v_id, maxResults=100)
                comment_res = comment_req.execute()
                comments = [c['snippet']['topLevelComment']['snippet']['textDisplay'] for c in comment_res.get('items', [])]
            except: pass 

            all_video_data.append({
                "video_id": v_id,
                "title": item['snippet']['title'],
                "views": int(stats.get('viewCount', 0)),
                "likes": int(stats.get('likeCount', 0)),
                "comments": comments
            })

        client.makedirs('/user/youtube')
        with client.write('/user/youtube/data.json', encoding='utf-8', overwrite=True) as writer:
            json.dump(all_video_data, writer)
        print("✅ COMPREHENSIVE INGESTION COMPLETE")
    except Exception as e: print(f"❌ FAILED: {e}")

fetch_comprehensive_data("Gaza 2024")