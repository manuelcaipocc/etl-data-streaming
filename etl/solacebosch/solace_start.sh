#!/bin/bash
echo "Checking Solace broker health..."
until curl -sf http://localhost:8080 || nc -z localhost 1883 || nc -z localhost 55555 || nc -z localhost 9000; do
  echo "Waiting for Solace broker..."
  sleep 5
done
echo "Solace broker is up and running!"
