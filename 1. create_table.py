"""
Задача 1.

Создать в sqlite пустую таблицу с полями

timestamp,
player_id,
event_id,
error_id,
json_server,
json_client

Не создаем PRIMARY KEY и индекс, потому что в ТЗ не указано.
"""

import sqlite3

# Устанавливаем соединение с базой данных
connection = sqlite3.connect('my_database.db')
cursor = connection.cursor()

# Создаем таблицу Users
cursor.execute('''
CREATE TABLE IF NOT EXISTS MyTable (
timestamp INTEGER NOT NULL,
player_id INTEGER NOT NULL,
event_id INTEGER NOT NULL,
error_id TEXT NOT NULL,
json_server TEXT,
json_client TEXT
)
''')

# Сохраняем изменения и закрываем соединение
connection.commit()
connection.close()
