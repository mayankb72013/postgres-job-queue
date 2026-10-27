# Postgres Job Queue

A durable background-job queue built on PostgreSQL, Node.js and TypeScript.
Jobs survive crashes, retry with exponential backoff, and are never claimed by two workers at once.

> Work in progress: this is the project skeleton. The full README comes at the end of the build.

## Quick start

    git clone <url> && cd postgres-job-queue
    npm install
    cp .env.example .env
    npm run db:up        # PostgreSQL 16 in Docker

Requires Node 22 and Docker.
