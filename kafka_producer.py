from kafka import KafkaProducer
import json
import time
import os
import multiprocessing
from tqdm import tqdm

# Cấu hình Kafka
KAFKA_TOPIC = "song_play_events"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Thư mục chứa dữ liệu JSON
DATA_DIR = "D:/data"

# Số lượng files cần xử lý song song
NUM_PROCESSES = 8

# Đọc danh sách file JSON
def get_json_files(directory):
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".json")]

# Gửi dữ liệu từ file JSON lên Kafka
def send_json_to_kafka(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Kiểm tra dữ liệu có playlists hay không
    if "playlists" in data:
        for playlist in data["playlists"]:
            for track in playlist["tracks"]:
                message = {
                    "playlist_id": playlist["pid"],
                    "track_id": track["track_uri"],
                    "track_name": track["track_name"],
                    "artist": track["artist_name"],
                    "genre": playlist.get("genre", "Unknown"),
                    "play_count": playlist.get("num_tracks", 1),
                    "timestamp": time.time()
                }

                producer.send(KAFKA_TOPIC, message)

# Xử lý nhiều files JSON song song
def process_files_parallel(files):
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        for _ in tqdm(pool.imap_unordered(send_json_to_kafka, files), total=len(files), desc="Processing files"):
            pass

if __name__ == "__main__":
    json_files = get_json_files(DATA_DIR)
    print(f"🔹 Tìm thấy {len(json_files)} files JSON. Đang gửi dữ liệu vào Kafka...")
    process_files_parallel(json_files)
    print("✅ Hoàn thành việc gửi dữ liệu vào Kafka!")