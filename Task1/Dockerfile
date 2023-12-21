FROM alpine:latest

RUN apk update && apk add --no-cache nginx

RUN apk add --no-cache sed

RUN apk add --no-cache shadow && \
    useradd -m sonoii && \
    groupadd sonoiiGroup && \
    usermod sonoii -G sonoiiGroup

RUN mkdir -p /var/www/my_project/img && \
    chown -R sonoii:sonoiiGroup /var/www/my_project

RUN apk add --no-cache curl && \
    curl -o /etc/nginx/nginx.conf https://raw.githubusercontent.com/nginx/nginx/master/conf/nginx.conf && \
    sed -i 's/user nginx/user sonoii/' /etc/nginx/nginx.conf && \
    sed -i 's/\/var\/www\/html/\/var\/www\/my_project/g' /etc/nginx/nginx.conf && \
    rm -rf /etc/nginx/conf.d/default.conf

COPY index.html /var/www/my_project/
COPY index.html /var/lib/nginx/html/
COPY img.jpg /var/www/my_project/img/

RUN chmod -R 755 /var/www/my_project

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]