# clean_and_merge_metadata.py

import sqlite3
import re

DB_FILE = "fingerprints.db"
DB_FILE = "figa.db"

def clean_text(text):
    if not text:
        return text
    t = text.lower()
    t = re.sub(r"\(.*?\)", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip().title()

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Load all songs
c.execute("SELECT id, title, artist FROM songs")
rows = c.fetchall()

clean_map = {}

for song_id, title, artist in rows:
    new_title = clean_text(title)
    new_artist = clean_text(artist)

    key = (new_title, new_artist)

    if key not in clean_map:
        clean_map[key] = song_id
        #print(f"Key={key} ID {song_id} \t title: {new_title} ")
        c.execute(
            "UPDATE songs SET title=?, artist=? WHERE id=?",
            (new_title, new_artist, song_id),
        )
    else:
        # Duplicate found
        keep_id = clean_map[key]
        duplicate_id = song_id

        print(f"Merging duplicate: {duplicate_id} -> {keep_id}")

        # Reassign fingerprints
        c.execute(
            "UPDATE fingerprints SET song_id=? WHERE song_id=?",
            (keep_id, duplicate_id),
        )

        # Delete duplicate song row
        c.execute("DELETE FROM songs WHERE id=?", (duplicate_id,))

conn.commit()
conn.close()

print("Cleaning and merging complete.")
