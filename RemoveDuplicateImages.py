import os
import sqlite3
from PIL import Image
import imagehash

# Directory containing the images
image_directory = 'image_directory'
# Path to your SQLite database file
db_path = 'database.db'

def get_image_hashes(directory):
    hashes = {}
    for filename in os.listdir(directory):
        if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            image_path = os.path.join(directory, filename)
            try:
                with Image.open(image_path) as img:
                    img_hash = imagehash.average_hash(img)
                    if img_hash in hashes:
                        hashes[img_hash].append(image_path)
                    else:
                        hashes[img_hash] = [image_path]
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    return hashes

def update_database_and_delete_duplicates(hashes, conn):
    cursor = conn.cursor()
    duplicates_removed = 0
    entries_updated = 0
    for paths in hashes.values():
        if len(paths) > 1:
            # Keep the first image and consider others as duplicates
            original = paths[0]
            original_filename = os.path.splitext(os.path.basename(original))[0]
            for duplicate in paths[1:]:
                duplicate_filename = os.path.splitext(os.path.basename(duplicate))[0]
                # Update database entries pointing to the duplicate to point to the original
                cursor.execute('''
                    UPDATE questions SET image_name = ? WHERE image_name = ?
                ''', (original_filename, duplicate_filename))
                entries_updated += cursor.rowcount
                # Delete the duplicate file
                os.remove(duplicate)
                duplicates_removed += 1
    conn.commit()
    return duplicates_removed, entries_updated

def main():
    conn = sqlite3.connect(db_path)
    hashes = get_image_hashes(image_directory)
    duplicates_removed, entries_updated = update_database_and_delete_duplicates(hashes, conn)
    conn.close()
    print(f"Duplicate images removed: {duplicates_removed}")
    print(f"Database entries updated: {entries_updated}")

if __name__ == "__main__":
    main()
