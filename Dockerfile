# Use official Go image as base
FROM golang:alpine

# Set working directory inside container
WORKDIR /app

# Copy source code into container
COPY . .

# Build Go application inside container
RUN go build -o lru_cache main.go

# Expose port for HTTP requests
EXPOSE 7171

# Command to run application when container starts
CMD ["./lru_cache"]