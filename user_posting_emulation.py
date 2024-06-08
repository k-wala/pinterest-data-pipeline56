import requests
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


class AWSDBConnector:

    def __init__(self):
        pass

    def read_db_creds(self):
        """
        Reads database credentials from a YAML file and returns them as a dictionary.
        """
        file_path = "/Users/Kronks/Documents/AiCore/pinterest/pinterest-data-pipeline56/db_creds.yaml"
        with open (file_path) as creds_file:
            db_creds_dict = yaml.safe_load(creds_file)
            return db_creds_dict
        
    def create_db_connector(self):
        db_creds = self.read_db_creds()
        database_type = 'mysql'
        dbapi = 'pymysql'
        host = db_creds['HOST']
        user = db_creds['USER']
        password = db_creds['PASSWORD']
        port = db_creds['PORT']
        database = db_creds['DATABASE']
        engine = sqlalchemy.create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")
        return engine    


new_connector = AWSDBConnector()

def send_data_to_kafka():
    
    engine = new_connector.create_db_connector()

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    with engine.connect() as connection:

        # # Iterate over pinterest_data table & send pinterest data to Kafka
        # pin_string = text("SELECT * FROM pinterest_data")
        # pin_rows = connection.execute(pin_string).fetchall()
        # pin_count = 0
        # pin_status = []
        # for pin_row in pin_rows:
        #     pin_result = dict(pin_row._mapping)
        #     pin_count += 1
        #     pin_payload = json.dumps({
        #         "records": [
        #             {
        #             "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"],
        #                     "title": pin_result["title"], "description": pin_result["description"],
        #                     "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"],
        #                     "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
        #                     "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"],
        #                     "save_location": pin_result["save_location"], "category": pin_result["category"]
        #                     }
        #             }
        #         ]
        #     })

        #     pin_invoke_url = "https://45wbr281ke.execute-api.us-east-1.amazonaws.com/test/topics/0affdc2332e7.pin"
        #     pin_response = requests.request("POST", pin_invoke_url, headers=headers, data=pin_payload)
        #     pin_status.append(pin_response.status_code)

        # print(f"Number of rows from pinterest_data submitted is: {pin_count}")

        # if all(status == 200 for status in pin_status):
        #     print("All pin data successfully sent to Kafka.")
        # else:
        #     print("Error: Failed to send some pin data to Kafka.")

        # Iterate over geolocation_data table & send geolocation data to Kafka
        # geo_string = text("SELECT * FROM geolocation_data")
        # geo_rows = connection.execute(geo_string).fetchall()
        # geo_count = 0
        # geo_status = []
        # for geo_row in geo_rows:
        #     geo_result = dict(geo_row._mapping)
        #     geo_count += 1
        #     geo_payload = json.dumps({
        #         "records": [
        #             {
        #             "value":{"index": geo_result["ind"], "country": geo_result["country"],
        #                     "timestamp": geo_result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
        #                     "latitude": geo_result["latitude"], "longitude": geo_result["longitude"]}
        #             }
        #         ]
        #     })

        #     geo_invoke_url = "https://45wbr281ke.execute-api.us-east-1.amazonaws.com/test/topics/0affdc2332e7.geo"
        #     geo_response = requests.request("POST", geo_invoke_url, headers=headers, data=geo_payload)
        #     geo_status.append(geo_response.status_code)

        # print(f"Number of rows from geolocation_data submitted is: {geo_count}")

        # if all(status == 200 for status in geo_status) == 200:
        #     print("All geo Data successfully sent to Kafka.")
        # else:
        #     print("Error: Failed to send some geo data to Kafka.")
        
        # # Iterate over user_data table & send user data to Kafka
        user_string = text("SELECT * FROM user_data")
        user_rows = connection.execute(user_string).fetchall()
        user_count = 0
        user_status = []
        for user_row in user_rows:
            user_result = dict(user_row._mapping)
            user_count += 1
            user_payload = json.dumps({
                "records": [
                    {
                    "value": {"index": user_result["ind"], "first_name": user_result["first_name"],
                            "last_name": user_result["last_name"], "age": user_result["age"],
                            "date_joined": user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")}
                    }
                ]
            })

            user_invoke_url = "https://45wbr281ke.execute-api.us-east-1.amazonaws.com/test/topics/0affdc2332e7.user"
            user_response = requests.request("POST", user_invoke_url, headers=headers, data=user_payload)
            user_status.append(user_response.status_code)

        if all(status == 200 for status in user_status):
                print("All user data successfully sent to Kafka.")
        else:
            print("Error: Failed to send some user data to Kafka.")    

        print(f"Number of rows from user_data submitted is: {user_count}")


send_data_to_kafka()


