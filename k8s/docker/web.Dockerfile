# Dev image: bun + nginx in a single stage so live_update can rebuild in-place
FROM oven/bun:1-alpine

RUN apk add --no-cache nginx && mkdir -p /run/nginx /usr/share/nginx/html

# Nginx config
COPY k8s/docker/nginx.conf /etc/nginx/http.d/default.conf

# Install dependencies (ignore optional native deps like usb)
WORKDIR /app
COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile --ignore-scripts

# Build
COPY web/ .
RUN bun run build && cp -r dist/* /usr/share/nginx/html/

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
