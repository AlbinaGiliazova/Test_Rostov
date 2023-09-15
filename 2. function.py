"""
Написать класс или функцию, которая:

1) Выгрузит данные из client.csv и server.scv за определенную дату.
2) Объединит данные из этих таблиц по error_id.
3) Исключит из выборки записи с player_id, которые есть в таблице  cheaters,
   но только в том случае если:
   у player_id ban_time - это предыдущие сутки или раньше относительно timestamp из server.scv
4) Выгрузит данные в таблицу, созданную в задаче 1. В ней должны бать следующие данные:

timestamp из server.csv
player_id из client.csv
error_id  из сджойненных server.csv и client.csv
json_server поле json из server.csv
json_client поле json из client.csv


Задача 3*.
Замерить потребление памяти во время выполнения задачи

Предположим, что речь про INNER JOIN.
Предположим, что нужно все же выгружать event_id, раз создаем для него столбец в таблице
"""
import csv
from datetime import datetime
import os
import psutil
import sqlite3


def func():

    max_memory = 0
    lines = {}
    with open('./task/server.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = (datetime.utcfromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d'))
            if date not in lines:
                lines[date] = []
            lines[date].append({'timestamp': row['timestamp'],
                                'event_id': row['event_id'],
                                'error_id': row['error_id'],
                                'json_server': row['description']})

        # замер потребления памяти в Мб
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
        max_memory = mem if mem > max_memory else max_memory

    count = 0
    count2 = 0
    count3 = 0
    with open('./task/client.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = (datetime.utcfromtimestamp(int(row['timestamp'])).strftime('%Y-%m-%d'))
            if date not in lines:
                count += 1
                continue
            #отбрасываем timestamp, потому что его надо брать из server.csv
            for line in lines[date]:
                if line['error_id'] == row['error_id']:
                    line['player_id'] = row['player_id']
                    line['json_client'] = row['description']
                    count3 += 1
                    break
            else:
                count2 += 1
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
        max_memory = mem if mem > max_memory else max_memory

    count4 = 0
    count5 = 0
    count6 = 0
    count7 = 0
    count8 = 0
    # Устанавливаем соединение с базой данных
    with sqlite3.connect('./task/cheaters.db') as connection:
        with sqlite3.connect('my_database.db') as connection2:

            cursor = connection.cursor()
            cursor2 = connection2.cursor()

            try:
                with connection2:

                    for date in lines:
                        for line in lines[date]:
                            if 'player_id' not in line:
                                count7 += 1
                                continue
                            try:
                                cursor.execute(f'SELECT ban_time FROM cheaters WHERE player_id IS {line["player_id"]}')
                                ban_time = cursor.fetchall()
                                if ban_time:
                                    ban_date = ban_time[0][0].split()[0]
                                    year, month, day = ban_date.split("-")
                                    ban_date = datetime(year=int(year), month=int(month), day=int(day)).date()
                                    if ban_date < datetime.utcfromtimestamp(int(line['timestamp'])).date():
                                        count4 += 1
                                        continue
                            except Exception as ex:
                                count8 += 1
                                print(ex)

                            cursor2.execute('INSERT INTO MyTable '
                                            '(timestamp, '
                                            'player_id,'
                                            'event_id,'
                                            'error_id,'
                                            'json_server,'
                                            'json_client) '
                                            'VALUES (?, ?, ?, ?, ?, ?)',
                                            (f"{line['timestamp']}",
                                             f"{line['player_id']}",
                                             f"{line['event_id']}",
                                             f"{line['error_id']}",
                                             f"{line['json_server']}",
                                             f"{line['json_client']}"))
                            count5 += 1

            except Exception as ex:
                print(ex)
                count6 += 1

    print(f"Отброшено {count} строк, которых нет в server.csv.")        # 0
    print(f"Не удалось объединить {count2} строк")                      # 33 325
    print(f"Удалось объединить {count3} строк")                         # 33 354
    print(f"Максимальное потребление памяти {round(max_memory)} Мб")    # 366
    print(f"Забанено {count4} пользователей")                           # 4 396
    print(f"Сделано {count5} записей")                                  # 28 958
    print(f"Возникло {count6} исключений при подключении к БД")         # 0
    print(f"{count7} строк отброшено, так как нет пары в client.csv")   # 33 321
    print(f"Возникло {count8} исключений при анализе банов")            # 0


if __name__ == "__main__":
    func()
