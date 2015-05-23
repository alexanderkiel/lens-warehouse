FROM gfredericks/leiningen:java-8-lein-2.5.1

COPY resources /app/resources
COPY src /app/src
COPY project.clj /app/
COPY docker/start.sh /app/

WORKDIR /app

RUN lein with-profile production,datomic-free deps
RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]
