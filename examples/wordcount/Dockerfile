# Build
FROM golang:1.20 as builder

# Install ssh client and git
RUN apt-get update && apt -y --no-install-recommends install openssh-client git
# Download public key for github.com
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
# change all requests to github from https to ssh
RUN git config --global url."ssh://git@github.com".insteadOf "https://github.com"

WORKDIR /app

# Retrieve application dependencies.
# This allows the container build to reuse cached dependencies.
# Expecting to copy go.mod and if present go.sum.
COPY go.* ./
RUN --mount=type=ssh go mod download

# Copy local code and run build
COPY . ./
RUN CGO_ENABLED=0 go build -o wordcount wordcount.go

# Final image
FROM alpine:3.15

WORKDIR /app
COPY --from=builder /app/wordcount .

RUN chmod +x wordcount

ENV input_files_dir "./input"
ENV intermediate_files_dir "./intermediate"
ENV output_files_dir "./output"
ENV coordinator_connection_string "coordinator:50051"
ENV rabbitmq_connection_string "amqp://guest:guest@rabbitmq"
ENV rabbitmq_queue "task_events"

CMD ["sh", "-c", "/app/wordcount ${input_files_dir} ${intermediate_files_dir} ${output_files_dir} ${coordinator_connection_string} ${rabbitmq_connection_string} ${rabbitmq_queue}"]
