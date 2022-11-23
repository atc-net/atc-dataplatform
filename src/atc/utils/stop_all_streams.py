from atc.spark import Spark


def stop_all_streams():
    for stream in Spark.get().streams.active:
        stream.stop()
        stream.awaitTermination()
