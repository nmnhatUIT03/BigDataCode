from kafka import KafkaConsumer
import json
import requests
import pymongo
import time
import itertools
import logging
from tqdm import tqdm

# --- Cấu hình ---
KAFKA_TOPIC = "song_play_events"
KAFKA_SERVER = "localhost:9092"
CONSUMER_GROUP = "music_consumer_group_" + str(int(time.time()))

LASTFM_API_KEYS = [
    "ce036b362619543bab6273666c041684",
    "e6519105e8142754954fef5f55c547f0",
    "3b3a8c7f16368d47ddd1e9052492671c",
]
LASTFM_URL = "http://ws.audioscrobbler.com/2.0/"

MONGO_URI = "mongodb+srv://21521226:c31Xo9jwa6JpslJK@bigdataserverlessmongo.3xgrnpa.mongodb.net/?retryWrites=true&w=majority&appName=BigdataServerlessMongo"
MONGO_DB = "music_recommendation"
MONGO_COLLECTION = "songs"

# --- Cấu hình Logging ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# --- Kiểm tra và tạo kết nối MongoDB ---
def connect_mongodb():
    try:
        mongo_client = pymongo.MongoClient(MONGO_URI)
        # Kiểm tra kết nối bằng cách ping
        mongo_client.admin.command('ping')
        db = mongo_client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        logging.info("✅ Kết nối MongoDB thành công!")
        return collection, None
    except Exception as e:
        logging.error(f"❌ Lỗi kết nối MongoDB: {e}")
        return None, str(e)

# --- Tạo Kafka Consumer ---
def create_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=10000,
        )
        logging.info(f"✅ Kết nối Kafka thành công với group {CONSUMER_GROUP}")
        return consumer, None
    except Exception as e:
        logging.error(f"❌ Lỗi kết nối Kafka: {e}")
        return None, str(e)

# --- Quản lý Last.fm API Keys ---
class LastFMAPI:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        
    def get_next_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return self.api_keys[self.current_key_index]
        
    def get_song_info(self, artist, track):
        max_retries = len(self.api_keys)
        retries = 0
        
        while retries < max_retries:
            current_key = self.api_keys[self.current_key_index]
            params = {
                "method": "track.getInfo",
                "api_key": current_key,
                "artist": artist,
                "track": track,
                "format": "json",
            }
            
            try:
                response = requests.get(LASTFM_URL, params=params, timeout=5)
                data = response.json()
                
                if "error" in data:
                    if "limit" in str(data.get("message", "")).lower():
                        logging.warning(f"⚠️ API Key {current_key[:8]}... bị giới hạn, đổi key...")
                        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
                        retries += 1
                        continue
                    else:
                        logging.error(f"❌ Last.fm API error: {data.get('message')}")
                        return None
                        
                if "track" in data:
                    track_data = data["track"]
                    return {
                        "listeners": int(track_data.get("listeners", 0)),
                        "playcount": int(track_data.get("playcount", 0)),
                        "genres": [tag["name"] for tag in track_data.get("toptags", {}).get("tag", [])] or ["Unknown"],
                    }
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"❌ HTTP Request error: {e}")
                retries += 1
                continue
                
            except Exception as e:
                logging.error(f"❌ Unexpected error calling Last.fm API: {e}")
                return None
                
        logging.error("❌ Đã thử tất cả API keys nhưng không thành công")
        return None

# --- Xử lý MongoDB Batch ---
class MongoDBBatch:
    def __init__(self, collection, batch_size=100):
        self.collection = collection
        self.batch_size = batch_size
        self.batch_data = []
        self.total_processed = 0
    
    def add(self, song):
        self.batch_data.append(
            pymongo.UpdateOne(
                {"track_id": song["track_id"]},
                {"$set": song},
                upsert=True
            )
        )
        
        if len(self.batch_data) >= self.batch_size:
            self.flush()
    
    def flush(self):
        if self.batch_data:
            try:
                self.collection.bulk_write(self.batch_data)
                self.total_processed += len(self.batch_data)
                logging.info(f"💾 Đã lưu batch ({len(self.batch_data)} bài hát) vào MongoDB. Tổng: {self.total_processed}")
                self.batch_data = []
            except Exception as e:
                logging.error(f"❌ Lỗi khi lưu vào MongoDB: {e}")

def process_messages(consumer, collection):
    lastfm = LastFMAPI(LASTFM_API_KEYS)
    mongo_batch = MongoDBBatch(collection)
    processed_count = 0
    
    try:
        logging.info("📥 Bắt đầu xử lý messages...")
        
        while True:
            try:
                # Đọc message với timeout
                message = next(consumer)
                if not message:
                    continue
                    
                song = message.value
                track_name = song.get("track_name", "Unknown")
                artist_name = song.get("artist", "Unknown")
                
                logging.info(f"🎵 Đang xử lý: {track_name} - {artist_name}")
                
                # Kiểm tra nếu bài hát đã tồn tại
                existing_song = collection.find_one({"track_id": song["track_id"]})
                if existing_song is not None:
                    logging.info(f"⏭️ Đã tồn tại: {track_name}, bỏ qua...")
                    continue
                
                # Lấy thông tin từ Last.fm
                song_info = lastfm.get_song_info(artist_name, track_name)
                if song_info:
                    song.update(song_info)
                    mongo_batch.add(song)
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        logging.info(f"💫 Đã xử lý {processed_count} bài hát")
                
            except StopIteration:
                logging.info("🏁 Hết messages để xử lý")
                break
                
            except Exception as e:
                logging.error(f"❌ Lỗi xử lý message: {e}")
                continue
                
    except Exception as e:
        logging.error(f"❌ Lỗi trong quá trình xử lý: {e}")
    finally:
        # Đảm bảo flush batch cuối cùng
        mongo_batch.flush()
        consumer.close()
        logging.info(f"🎯 Tổng số bài hát đã xử lý: {processed_count}")

def main():
    # Kết nối MongoDB
    collection, mongo_error = connect_mongodb()
    if mongo_error is not None:
        logging.error(f"Không thể tiếp tục vì lỗi MongoDB: {mongo_error}")
        return

    # Tạo consumer
    consumer, kafka_error = create_consumer()
    if kafka_error is not None:
        logging.error(f"Không thể tiếp tục vì lỗi Kafka: {kafka_error}")
        return
        
    # Xử lý messages
    process_messages(consumer, collection)

if __name__ == "__main__":
    main()