FROM node:18-slim

WORKDIR /app

COPY evidence/package*.json ./
RUN npm install

COPY evidence/ .

EXPOSE 3000

ENTRYPOINT ["npm", "run", "dev", "--", "--host", "0.0.0.0"]