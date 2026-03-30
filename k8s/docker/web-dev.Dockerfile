# Dev image: runs Vite dev server with HMR instead of nginx static serving
FROM oven/bun:1-alpine

WORKDIR /app
COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile --ignore-scripts

COPY web/ .

ENV VITE_API_URL=http://api.lake-dev.svc.cluster.local:8080

EXPOSE 5173
CMD ["bun", "run", "dev", "--host", "0.0.0.0", "--port", "5173"]
