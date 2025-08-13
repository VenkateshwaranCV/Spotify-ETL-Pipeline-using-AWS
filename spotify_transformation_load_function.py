import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_by = row['track']['artists'][0]['name']
        song_relese_date = row['track']['album']['release_date']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['album']['external_urls']['spotify']
        
        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'song_by': song_by,
            'song_relese_date': song_relese_date,
            'song_duration': song_duration,
            'song_url': song_url
        }
        
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket='spotify-etl-project-suryavenkat'
    Key='raw_data/to_processed/'

    spotify_data =[]
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        song_list = songs(data)
        song_df = pd.DataFrame.from_dict(song_list)
        song_df = song_df.drop_duplicates(subset=['song_id'])

        song_df['song_relese_date'] = pd.to_datetime(song_df['song_relese_date'])

        song_key = 'transformed_data/songs_data/song_transformed_' + str(datetime.now) + '.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False) #gluecrawler can't detect index
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
            copy_source = {
                'Bucket': Bucket,
                'Key': key
            }
            s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
            s3_resource.Object(Bucket, key).delete()   


    

            
            


            


