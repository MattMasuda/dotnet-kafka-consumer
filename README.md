# DotNetKafkaConsumer

This is a sample Kafka consumer using the Confluent Kafka client for .NET. It is a simple console application that subscribes to a topic and prints out received messages. By adjusting the configuration properties you can use it to connect to Bluemix Message Hub or to a vanilla Kafka cluster.

## How to run it
You should be able to clone or download the repo, open it in Visual Studio, and build it straight away. The solution has dependencies on the Confluent.Kafka and librdkafka.redist Nuget packages. The app.config file contains settings for the bootstrap servers, consumer group ID, and topic.  If you are connecting to Message Hub you will have a username and password that you need to pass as arguments on the command line.

## What's the deal with this Digicert root certificate?
This is for Message Hub, which requires SSL. The librdkafka library doesn't have access to certificate stores provided by the environment, so you have to provide a root certificate so that it can validate the certificates presented by the Message Hub brokers. If you're connecting to a normal Kafka cluster this isn't required unless you are using SSL, which is beyond the scope of this example. The certificate itself was exported from the certificate store on a Windows 7 machine; if you don't trust it feel free to use a different one.
