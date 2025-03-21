from pyspark.sql import SparkSession 
import json

if __name__ == "__main__":
    # Crear una sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("monkeypox_tweets")\
        .getOrCreate()

    print("read dataset.csv ... ")
    # Ruta del dataset de tweets
    path_tweets = "monkeypox_tweets.csv"
    df_tweets = spark.read.csv(path_tweets, header=True, inferSchema=True)
    
    # Renombramos algunas columnas si es necesario, por ejemplo, "tweet" a "text"
    df_tweets = df_tweets.withColumnRenamed("tweet", "text")
    
    # Crear vista temporal para consultas SQL
    df_tweets.createOrReplaceTempView("tweets")
    
    # Mostrar la estructura del dataset
    query = 'DESCRIBE tweets'
    spark.sql(query).show(20)

    # Consultar los tweets en inglés
    query = """SELECT id, text, language, date, time FROM tweets WHERE language = 'en' ORDER BY date, time"""
    df_tweets_en = spark.sql(query)
    df_tweets_en.show(20)

    # Filtrar los tweets de una fecha específica, por ejemplo, tweets del 2023
    query = """SELECT id, text, date, time FROM tweets WHERE date BETWEEN '2023-01-01' AND '2023-12-31' ORDER BY date"""
    df_tweets_2023 = spark.sql(query)
    df_tweets_2023.show(20)

    # Guardar los tweets filtrados en un archivo JSON
    results = df_tweets_2023.toJSON().collect()
    with open('results/monkeypox_tweets_2023.json', 'w') as file:
        json.dump(results, file)

    # Consultar el número de respuestas, retweets y likes por tweet
    query = """SELECT id, replies_count, retweets_count, likes_count FROM tweets ORDER BY likes_count DESC"""
    df_tweets_engagement = spark.sql(query)
    df_tweets_engagement.show(20)

    # Consultar el número total de tweets por idioma
    query = """SELECT language, COUNT(id) FROM tweets GROUP BY language"""
    df_tweets_language_count = spark.sql(query)
    df_tweets_language_count.show()

    # Detener la sesión de Spark
    spark.stop()
