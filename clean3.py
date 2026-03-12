import sqlite3
import re
from collections import defaultdict

DB_FILE = "fingerprints.db"

def clean_text(text):
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"\(.*?\)", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip().title()

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

print("Loading songs...")

c.execute("SELECT id, title, artist FROM songs")
rows = c.fetchall()

# Step 1: Build FULL cleaned grouping globally
groups = defaultdict(list)

for song_id, title, artist in rows:
    cleaned_title = clean_text(title)
    cleaned_artist = clean_text(artist)
    key = (cleaned_title, cleaned_artist)
    groups[key].append(song_id)

print(f"Found {len(groups)} unique cleaned keys")

# Step 2: Process each cleaned group safely
for (new_title, new_artist), ids in groups.items():

    if len(ids) == 1:
        # Only one entry — just update safely
        song_id = ids[0]

        try:
            c.execute(
                "UPDATE songs SET title=?, artist=? WHERE id=?",
                (new_title, new_artist, song_id)
            )
        except sqlite3.IntegrityError:
            print(f"Skipping update collision for ID {song_id}")
        continue

    # Multiple IDs → merge required
    keep_id = min(ids)
    print(f"\nMerging group into {keep_id} ({new_title} - {new_artist})")

    # Move fingerprints from all duplicates
    for duplicate_id in ids:
        if duplicate_id == keep_id:
            continue

        print(f"  Merging duplicate: {duplicate_id} -> {keep_id}")

        c.execute(
            "UPDATE fingerprints SET song_id=? WHERE song_id=?",
            (keep_id, duplicate_id)
        )

        c.execute(
            "DELETE FROM songs WHERE id=?",
            (duplicate_id,)
        )

    # Finally update the master row
    c.execute(
        "UPDATE songs SET title=?, artist=? WHERE id=?",
        (new_title, new_artist, keep_id)
    )

conn.commit()
conn.close()

print("\nCleaning + merging complete.")
