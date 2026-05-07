# Token Transfer Monitor

Backend service dùng Go, RabbitMQ và PostgreSQL để theo dõi sự kiện chuyển token ERC-20 trên blockchain EVM, đưa event vào queue, xử lý nền bằng worker, lưu dữ liệu vào database, hỗ trợ retry, DLQ, idempotency và graceful shutdown.

## 1. Mục tiêu đồ án

Đồ án xây dựng một backend pipeline có khả năng:

- Lắng nghe event `Transfer(address indexed from, address indexed to, uint256 value)` của ERC-20 token.
- Publish event blockchain vào RabbitMQ.
- Worker consume event từ queue và xử lý nền.
- Lưu transfer activity vào PostgreSQL.
- Chống xử lý trùng bằng `processed_events`.
- Retry lỗi tạm thời bằng retry queue + TTL.
- Đưa message lỗi vĩnh viễn vào DLQ.
- Dùng publisher confirms để giảm rủi ro mất message khi publish retry.
- Worker có reconnect loop và graceful shutdown.
- Load danh sách token theo dõi và ví theo dõi từ PostgreSQL.

## 2. Tech stack

- Go
- RabbitMQ
- PostgreSQL
- Docker Compose
- pgx / pgxpool
- go-ethereum
- AMQP 0-9-1 client: `github.com/rabbitmq/amqp091-go`

## 3. Kiến trúc tổng quan

```text
EVM WebSocket RPC
        |
        v
Blockchain Listener
        |
        | publish token.transfer.detected
        v
RabbitMQ Exchange: token.events
        |
        v
Queue: token.transfer.process
        |
        v
Transfer Worker
        |
        +--> PostgreSQL: transfer_activities
        |
        +--> PostgreSQL: processed_events
        |
        +--> Retry Queue nếu lỗi tạm thời
        |
        +--> DLQ nếu lỗi vĩnh viễn hoặc quá số lần retry