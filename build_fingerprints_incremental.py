import os
import sys
import sqlite3
import librosa
import numpy as np
from mutagen import File
from scipy.ndimage import maximum_filter

DB_FILE = "fingerprints.db"

FAN_VALUE = 5
PEAK_NEIGHBORHOOD = 20
MIN_HASH_TIME_DELTA = 0
MAX_HASH_TIME_DELTA = 200
COMMIT_INTERVAL = 25  # commit every 25 songs


# =========================
# DB INIT (safe for batches)
# =========================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY,
            title TEXT,
            artist TEXT,
            UNIQUE(title, artist)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS fingerprints (
            hash INTEGER,
            song_id INTEGER,
            offset INTEGER
        )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_hash ON fingerprints(hash)")

    # Speed up inserts
    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA journal_mode = MEMORY")

    conn.commit()
    return conn


# =========================
# Metadata
# =========================
def get_metadata(filepath):
    audio = File(filepath, easy=True)

    if not audio:
        return os.path.splitext(os.path.basename(filepath))[0], "Unknown"

    title = audio.get("title", [None])[0]
    artist = audio.get("artist", [None])[0]

    if not title:
        title = os.path.splitext(os.path.basename(filepath))[0]
    if not artist:
        artist = "Unknown"

    return title.strip().lower(), artist.strip().lower()


# =========================
# Peak detection
# =========================
def get_peaks(y, sr):
    S = librosa.feature.melspectrogram(y=y, sr=sr)
    S_db = librosa.power_to_db(S)

    local_max = maximum_filter(S_db, size=PEAK_NEIGHBORHOOD) == S_db
    peaks = np.argwhere(local_max & (S_db > np.percentile(S_db, 75)))

    return peaks


# =========================
# Hash generation
# =========================
def generate_hashes(peaks):
    hashes = []

    for i in range(len(peaks)):
        for j in range(1, FAN_VALUE):
            if i + j < len(peaks):
                f1, t1 = peaks[i]
                f2, t2 = peaks[i + j]
                dt = t2 - t1

                if MIN_HASH_TIME_DELTA <= dt <= MAX_HASH_TIME_DELTA:
                    h = (int(f1) << 20) | (int(f2) << 10) | int(dt)
                    hashes.append((h, int(t1)))

    return hashes


# =========================
# Build one folder
# =========================
def process_folder(folder):
    conn = init_db()
    c = conn.cursor()

    song_count = 0

    for root, _, files in os.walk(folder):
        for file in files:
            if not file.lower().endswith((".mp3", ".wav", ".flac", ".m4a")):
                continue

            path = os.path.join(root, file)
            print(f"Processing: {path}")

            title, artist = get_metadata(path)

            # Insert or ignore duplicate
            c.execute(
                "INSERT OR IGNORE INTO songs(title, artist) VALUES (?, ?)",
                (title, artist)
            )

            c.execute(
                "SELECT id FROM songs WHERE title=? AND artist=?",
                (title, artist)
            )
            song_id = c.fetchone()[0]

            y, sr = librosa.load(path, mono=True)
            peaks = get_peaks(y, sr)
            hashes = generate_hashes(peaks)

            c.executemany(
                "INSERT INTO fingerprints VALUES (?, ?, ?)",
                [(h, song_id, offset) for h, offset in hashes]
            )

            song_count += 1

            if song_count % COMMIT_INTERVAL == 0:
                conn.commit()

    conn.commit()
    conn.close()

    print(f"Finished processing {song_count} songs.")
    

# =========================
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python build_fingerprints_incremental.py <folder>")
        sys.exit()

    process_folder(sys.argv[1])
