CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;


CREATE TABLE IF NOT EXISTS raw.repositories (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    full_name VARCHAR(255),
    lang VARCHAR(25),
    stargazers_count BIGINT,
    forks_count BIGINT,
    open_issues_count INTEGER,
    topics JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    collected_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.repositories (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    full_name VARCHAR(255),
    lang VARCHAR(25),
    stargazers_count BIGINT,
    forks_count BIGINT,
    open_issues_count INTEGER,
    topics JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    collected_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.top_repos (
    repo_id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    forks_count BIGINT,
    stargazers_count BIGINT,
    lang VARCHAR(25)
);