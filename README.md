Проект: выгрузка данных об игроках из базы данных, с выполнением условий.

Есть два csv файла client.csv и server.csv
В sqlite есть таблица cheaters.

Поля client.csv:

    timestamp ,
    player_id ,
    error_id ,
    json

Поля server.csv:
    timestamp ,
    event_id ,
    error_id ,
    json

Поля таблицы cheaters:
 player_id integer,
 ban_time string



Задача 1.

Создать в sqlite пустую таблицу с полями

timestamp,
player_id,
event_id,
error_id,
json_server,
json_client



Задача 2.

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


Можно использовать любые библиотеки
Можно гуглить
Большим плюсом является структурированность кода и соответствие PEP
