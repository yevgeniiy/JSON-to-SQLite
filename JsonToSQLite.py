import sqlite3
import json
import os

# Path to your directory containing JSON files
json_directory = 'json_directory'

# Path to your directory containing image files
image_directory = 'image_directory'

# Function to load JSON data from a file
def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('database.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS questions (
    id INTEGER PRIMARY KEY,
    question TEXT,
    correct_answer TEXT,
    answer2 TEXT,
    answer3 TEXT,
    answer4 TEXT,
    image_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT,
    category_name TEXT,
    level_name TEXT,
    UNIQUE(group_name, category_name, level_name)
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS question_categories (
    question_id INTEGER,
    category_id INTEGER,
    FOREIGN KEY (question_id) REFERENCES questions (id),
    FOREIGN KEY (category_id) REFERENCES categories (id),
    PRIMARY KEY (question_id, category_id)
)''')

def insert_data(data):
    question_count = 0
    ignored_relations_count = 0
    for entry in data:
        full_category = entry['table']
        group, category, level = full_category.split('_')
        questions = entry['data']
        
        cursor.execute('''
        INSERT OR IGNORE INTO categories (group_name, category_name, level_name) VALUES (?, ?, ?)
        ''', (group, category, level))
        cursor.execute('''
        SELECT id FROM categories WHERE group_name = ? AND category_name = ? AND level_name = ?
        ''', (group, category, level))
        category_id = cursor.fetchone()[0]

        for question in questions:
            image_file = f"{question['Id']}.jpg"
            image_path = os.path.join(image_directory, image_file)
            image_name = os.path.splitext(image_file)[0]  # Extract the filename without extension

            if not os.path.exists(image_path):
                image_name = None  # Or handle as required

            cursor.execute('SELECT id FROM questions WHERE id = ?', (question['Id'],))
            existing_question = cursor.fetchone()

            if existing_question is None:
                cursor.execute('''
                INSERT INTO questions (id, question, correct_answer, answer2, answer3, answer4, image_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (question['Id'], question['Question'], question['Correct_Answer'],
                      question['Answer2'], question['Answer3'], question['Answer4'], image_name))
                question_count += 1

            cursor.execute('''
            INSERT OR IGNORE INTO question_categories (question_id, category_id)
            VALUES (?, ?)
            ''', (question['Id'], category_id))
            if cursor.rowcount == 0:
                ignored_relations_count += 1

    conn.commit()
    return question_count, ignored_relations_count

total_questions = 0
total_ignored_relations = 0
for filename in os.listdir(json_directory):
    if filename.endswith('.json'):
        file_path = os.path.join(json_directory, filename)
        data = load_json(file_path)
        questions_added, relations_ignored = insert_data(data)
        total_questions += questions_added
        total_ignored_relations += relations_ignored

print(f'Total {total_questions} questions have been parsed and added to the database.')
print(f'Total {total_ignored_relations} relations were ignored due to duplicates.')

conn.close()
