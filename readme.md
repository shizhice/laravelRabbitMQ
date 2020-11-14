# RabbitMQ for laravel queue

## RabbitMQ配置

配置`用户名`和`vhost`

## laravel配置

在`config/app.php`增加`LaravelRabbitMQ\RabbitMQQueueServiceProvider::class`

## `.env`配置如下

```
QUEUE_DRIVER=rabbitmq-worker

RABBITMQ_HOST=127.0.0.1
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_VHOST=guest_vhost

RABBITMQ_2_HOST=127.0.0.1
RABBITMQ_2_PORT=5673
RABBITMQ_2_USER=guest
RABBITMQ_2_PASSWORD=guest
RABBITMQ_2_VHOST=/
```

### 简单队列模式

queue连接 rabbitmq-simple
```shell
php artisan rabbitmq:test --mode=simple --num=3 --queue=simple_queue_1
php artisan rabbitmq:consume rabbitmq-simple --queue=simple_queue_1 --timeout=0
```
### worker模式

queue连接  rabbitmq-worker
```shell
php artisan rabbitmq:test --mode=worker --num=3 --queue=worker_queue_1
php artisan rabbitmq:consume rabbitmq-worker --queue=worker_queue_1 --timeout=0
```
