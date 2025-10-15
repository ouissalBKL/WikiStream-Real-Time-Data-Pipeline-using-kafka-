from kafka import KafkaConsumer
import pyodbc
from datetime import datetime


def consum():
    bootstrap_servers = "localhost:9092"
    topic = "wiki-changes"

    consumer = KafkaConsumer(
        topic,
        group_id="wiki_groupe",
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    try:
        cnxn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DESKTOP-FM06RCQ;"
            "Database=wiki_changes_db;"
            "Trusted_Connection=yes;"
        )
        sql_connection = pyodbc.connect(cnxn_str)
        sql_cursor = sql_connection.cursor()

        for message in consumer:
            try:
                values = message.value.split(",")

                # Exemple: vérifier le nombre de valeurs
                if len(values) != 7:
                    print(
                        f"Message ignoré, nombre inattendu de valeurs : {values} - consumer.py:106"
                    )
                    continue

                # unpacking
                ts, wiki, username, title, type_change, namespace_id, bot = values

                # formatage datetime
                formatted_datetime = ts.split(".")[0]
                formatted_datetime = datetime.strptime(
                    formatted_datetime, "%Y-%m-%d %H:%M:%S"
                )

                insert_query = """
                    INSERT INTO wiki_changes (ts, wiki, username, title, type_change, namespace_id, bot)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """

                data_to_insert = (
                    formatted_datetime,
                    wiki,
                    username,
                    title,
                    type_change,
                    int(namespace_id),
                    1 if bot.strip().lower() == "true" else 0,
                )

                sql_cursor.execute(insert_query, data_to_insert)
                sql_connection.commit()

                print(
                    f"Inserted data at {formatted_datetime} into SQL Server - consumer.py:136"
                )

            except Exception as e:
                print(
                    f"Erreur lors de l'insertion dans la base de données: {e} - consumer.py:139"
                )

    except KeyboardInterrupt:
        print("Consumer stopped - consumer.py:142")
    finally:
        sql_cursor.close()
        sql_connection.close()
        print("SQL Server connection closed. - consumer.py:146")
