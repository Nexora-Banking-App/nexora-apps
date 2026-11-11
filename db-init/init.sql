CREATE DATABASE IF NOT EXISTS nexora_bank;
USE nexora_bank;

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS idempotency_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    status ENUM('PROCESSING', 'COMPLETED') NOT NULL,
    lease_version INT NOT NULL DEFAULT 1,
    response_body JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_user_idempotency (user_id, idempotency_key),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    idempotency_key VARCHAR(64) NOT NULL,
    sender_id INT NOT NULL,
    receiver_id INT NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sender_id) REFERENCES users(id),
    FOREIGN KEY (receiver_id) REFERENCES users(id)
);

-- Transactional outbox: transfer events waiting to be published to Kafka.
-- Same definition as the db-init Jobs in app-manifests (overlays/*/db-init-job.yaml).
CREATE TABLE IF NOT EXISTS outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    topic VARCHAR(100) NOT NULL,
    event_key VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP NULL,
    INDEX idx_unpublished (published_at, id)
);

-- SEED: Nexora Central Bank Treasury Reserve (User ID 1)
INSERT INTO users (id, username, email, phone, password_hash)
VALUES (1, 'nexora_treasury', 'treasury@nexora.bank', '000-000-0000', '$2b$12$e/samplehashedpasswordfortreasurynotforlogin')
ON DUPLICATE KEY UPDATE id=id;

INSERT INTO accounts (user_id, balance)
VALUES (1, 10000000.00)
ON DUPLICATE KEY UPDATE user_id=user_id;