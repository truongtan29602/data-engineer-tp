FROM ubuntu

RUN apt-get update && apt-get install -y maven openjdk-8-jdk

COPY . .

EXPOSE 8080

RUN mvn verify

CMD ["java -Xmx8m -Xms8m -jar words.jar"]