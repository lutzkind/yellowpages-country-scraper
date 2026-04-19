FROM node:22-bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV HOST=0.0.0.0
ENV PORT=3000

RUN mkdir -p /app/data /app/data/exports
VOLUME ["/app/data"]

EXPOSE 3000

CMD ["node", "index.js"]
