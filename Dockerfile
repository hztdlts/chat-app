FROM node:18-alpine

WORKDIR /app

# Copy package files and install dependencies
COPY package.json ./
RUN npm install --production

# Copy all source code
COPY . .

# Create data and uploads directories
RUN mkdir -p data uploads

# Expose port (ClawCloud will set PORT env variable)
EXPOSE 3001

# Start the server
CMD ["node", "server.js"]
