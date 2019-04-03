from kafka import KafkaConsumer
consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')
for msg in consumer:
    print (msg.value)
    #receive message
    #download file from s3 for given sku
    #resize images
    #upload images to s3
    #commit message
    #try catch
    #send respone to api.
