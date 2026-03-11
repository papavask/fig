import sqlite3
import re
from collections import defaultdict

DB_FILE = "figa.db"

def clean_text(text):
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"\(.*?\)", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip().title()

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

c.execute("SELECT id, title, artist FROM songs")
rows = c.fetchall()

# Build cleaned groups first
groups = defaultdict(list)

for song_id, title, artist in rows:
    cleaned_title = clean_text(title)
    cleaned_artist = clean_text(artist)
    key = (cleaned_title, cleaned_artist)
    groups[key].append(song_id)

print("Processing groups...")

for (new_title, new_artist), ids in groups.items():

    # Keep lowest ID as master
    keep_id = min(ids)

    # Update master to cleaned name
    c.execute(
        "UPDATE songs SET title=?, artist=? WHERE id=?",
        (new_title, new_artist, keep_id)
    )

    # Merge duplicates
    for duplicate_id in ids:
        if duplicate_id == keep_id:
            continue

        print(f"Merging duplicate: {duplicate_id} -> {keep_id}")

        # Move fingerprints
        c.execute(
            "UPDATE fingerprints SET song_id=? WHERE song_id=?",
            (keep_id, duplicate_id)
        )

        # Delete duplicate
        c.execute(
            "DELETE FROM songs WHERE id=?",
            (duplicate_id,)
        )

conn.commit()
conn.close()

print("Cleaning and merging complete.")
