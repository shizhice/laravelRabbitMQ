<?php


namespace LaravelRabbitMQ;


use Arr;
use Str;
use Exception;
use ErrorException;
use Illuminate\Queue\Queue;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use LaravelRabbitMQ\Jobs\RabbitMQJob;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Connection\AbstractConnection;
use LaravelRabbitMQ\Exceptions\RabbitMQException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class RabbitMQQueue  extends Queue implements QueueContract
{
    /**
     * The RabbitMQ connection instance.
     *
     * @var AbstractConnection
     */
    protected $connection;

    /**
     * The RabbitMQ channel instance.
     *
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $defaultQueue;

    /**
     * @var array
     */
    protected $options;

    /**
     * @var array
     */
    protected $config;

    /**
     * List of already bound queues to exchanges.
     *
     * @var array
     */
    protected $boundQueues = [];

    /**
     * List of already declared queues.
     *
     * @var array
     */
    protected $queues = [];

    /**
     * List of already declared exchanges.
     *
     * @var array
     */
    protected $exchanges = [];

    /**
     * Current job being processed.
     *
     * @var RabbitMQJob
     */
    protected $currentJob;

    /**
     * RabbitMQQueue constructor.
     * @param $connection
     * @param $defaultQueue
     * @param $options
     * @param $config
     */
    public function __construct(AbstractConnection $connection, $defaultQueue, $options, $config)
    {
        $this->connection = $connection;
        $this->channel = $connection->channel();
        $this->defaultQueue = $defaultQueue;
        $this->options = $options;
        $this->config = $config;
    }

    /**
     * @inheritDoc
     * @throws AMQPProtocolChannelException
     */
    public function size($queue = null)
    {
        $queue = $this->getQueue($queue);

        if (! $this->isQueueExists($queue)) {
            return 0;
        }

        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->connection->channel();
        [, $size] = $channel->queue_declare($queue, true);
        $channel->close();

        return $size;
    }

    /**
     * @inheritDoc
     * @throws RabbitMQException
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, []);
    }

    /**
     * @inheritDoc
     * @throws RabbitMQException
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        $this->channel->basic_publish($message, $exchange, $destination, true, false);

        return $correlationId;
    }

    /**
     * Declare the destination when necessary.
     * @param string $destination
     * @param string|null $exchange
     * @param string $exchangeType
     * @throws RabbitMQException
     */
    protected function declareDestination(string $destination, ?string $exchange = null, string $exchangeType = AMQPExchangeType::DIRECT)
    {
        $class = get_class($this);
        throw new RabbitMQException("class $class must implements function 'declareDestination'.");
    }

    /**
     * @param $queue
     * @return RabbitMQQueue
     * @throws RabbitMQException
     */
    public function configurationConsume($queue): RabbitMQQueue
    {
        $class = get_class($this);
        throw new RabbitMQException("class $class must implements function 'configurationConsume'.");
    }

    /**
     * @inheritDoc
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->laterRaw(
            $delay,
            $this->createPayload($job, $queue, $data),
            $queue
        );
    }

    /**
     * @param $delay
     * @param $payload
     * @param null $queue
     * @param int $attempts
     * @return mixed
     * @throws RabbitMQException
     */
    public function laterRaw($delay, $payload, $queue = null, $attempts = 0)
    {
        $ttl = $this->secondsUntil($delay) * 1000;

        // When no ttl just publish a new message to the exchange or queue
        if ($ttl <= 0) {
            return $this->pushRaw($payload, $queue, ['delay' => $delay, 'attempts' => $attempts]);
        }

        $destination = $this->getQueue($queue).'.delay.'.$ttl;

        $this->declareQueue($destination, true, false, $this->getDelayQueueArguments($this->getQueue($queue), $ttl));

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        // Publish directly on the delayQueue, no need to publish trough an exchange.
        $this->channel->basic_publish($message, null, $destination, true, false);

        return $correlationId;
    }

    /**
     * @inheritDoc
     * @throws AMQPProtocolChannelException
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueue($queue);

            /** @var AMQPMessage|null $message */
            if ($message = $this->channel->basic_get($queue)) {
                return $this->currentJob = new RabbitMQJob(
                    $this->container,
                    $this,
                    $message,
                    $this->connectionName,
                    $queue
                );
            }
        } catch (AMQPProtocolChannelException $exception) {
            // If there is not exchange or queue AMQP will throw exception with code 404
            // We need to catch it and return null
            if ($exception->amqp_reply_code === 404) {

                // Because of the channel exception the channel was closed and removed.
                // We have to open a new channel. Because else the worker(s) are stuck in a loop, without processing.
                $this->channel = $this->connection->channel();

                return null;
            }

            throw $exception;
        }

        return null;
    }

    /**
     * Close the connection to RabbitMQ.
     * @throws Exception
     */
    public function close()
    {
        if ($this->currentJob && ! $this->currentJob->isDeletedOrReleased()) {
            $this->reject($this->currentJob, true);
        }

        try {
            $this->connection->close();
        } catch (ErrorException $exception) {
            // Ignore the exception
        }
    }

    /**
     * Reject the message.
     *
     * @param RabbitMQJob $job
     * @param bool $requeue
     *
     * @return void
     */
    public function reject(RabbitMQJob $job, bool $requeue = false): void
    {
        $this->channel->basic_reject($job->getRabbitMQMessage()->getDeliveryTag(), $requeue);
    }

    /**
     * Create a AMQP message.
     *
     * @param $payload
     * @param int $attempts
     * @return array
     */
    protected function createMessage($payload, int $attempts = 0): array
    {
        $properties = [
            'content_type' => 'application/json', // 消息体类型
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, // 消息持久化机制(程序中断，也不会丢失数据)
        ];

        // 获取队列ID
        if ($correlationId = json_decode($payload, true)['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
        }

        // 延迟队列
        if ($this->isPrioritizeDelayed()) {
            // 使用priority属性发布优先级消息，数字越大代表优先级越高
            // 如果优先级队列priority属性被设置为比x-max-priority大，那么priority的值被设置为x-max-priority的值。
            $properties['priority'] = $attempts;
        }

        $message = new AMQPMessage($payload, $properties);

        $message->set('application_headers', new AMQPTable([
            'laravel' => [
                'attempts' => $attempts,
            ],
        ]));

        return [
            $message,
            $correlationId,
        ];
    }

    /**
     * Returns &true;, if delayed messages should be prioritized.
     *
     * @return bool
     */
    protected function isPrioritizeDelayed(): bool
    {
        return boolval(Arr::get($this->options, 'prioritize_delayed') ?: false);
    }

    /**
     * Determine all publish properties.
     *
     * @param $queue
     * @param array $options
     * @return array
     */
    public function publishProperties($queue, array $options = []): array
    {
        $queue = $this->getQueue($queue);
        $attempts = Arr::get($options, 'attempts') ?: 0;

        $destination = $this->getRoutingKey($queue);
        $exchange = $this->getExchange();
        $exchangeType = $this->getExchangeType();

        return [$destination, $exchange, $exchangeType, $attempts];
    }

    /**
     * Gets a queue/destination, by default the queue option set on the connection.
     *
     * @param null $queue
     * @return string
     */
    public function getQueue($queue = null)
    {
        return $queue ?: $this->defaultQueue;
    }

    /**
     * Get the exchange name, or &null; as default value.
     *
     * @param string $exchange
     * @return string|null
     */
    protected function getExchange(string $exchange = null): ?string
    {
        return $exchange ?: Arr::get($this->options, 'exchange') ?: null;
    }

    /**
     * Get the routing-key for when you use exchanges
     * The default routing-key is the given destination.
     *
     * @param string $destination
     * @return string
     */
    protected function getRoutingKey(string $destination): string
    {
        return ltrim(sprintf(Arr::get($this->options, 'exchange_routing_key') ?: '%s', $destination), '.');
    }

    /**
     * Get the exchangeType, or AMQPExchangeType::DIRECT as default.
     *
     * @param string|null $type
     * @return string
     */
    protected function getExchangeType(?string $type = null): string
    {
        return @constant(AMQPExchangeType::class.'::'.Str::upper($type ?: Arr::get($this->options, 'exchange_type') ?: 'direct')) ?: AMQPExchangeType::DIRECT;
    }

    /**
     * Checks if the given queue already present/defined in RabbitMQ.
     * Returns false when when the queue is missing.
     *
     * @param string $name
     * @return bool
     * @throws AMQPProtocolChannelException
     */
    public function isQueueExists(string $name = null): bool
    {
        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->connection->channel();
            $channel->queue_declare($this->getQueue($name), true);
            $channel->close();

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a queue in rabbitMQ, when not already declared.
     *
     * @param string $name
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     * @return void
     */
    public function declareQueue(string $name, bool $durable = true, bool $autoDelete = false, array $arguments = []): void
    {
        // 已经声明的队列不再声明
        if ($this->isQueueDeclared($name)) {
            return;
        }

        $this->channel->queue_declare(
            $name,
            false,
            $durable,
            false,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );

        $this->queues[] = $name;
    }

    /**
     * Checks if the queue was already declared.
     *
     * @param string $name
     * @return bool
     */
    protected function isQueueDeclared(string $name): bool
    {
        return in_array($name, $this->queues, true);
    }

    /**
     * Get the Queue arguments.
     *
     * @param string $destination
     * @return array
     */
    protected function getQueueArguments(string $destination): array
    {
        $arguments = [];

        // 没有优先级属性的消息将被视为它们的优先级为0。优先级高于队列最大优先级的消息将被视为优先级，以最高优先级发布。
        if ($this->isPrioritizeDelayed()) {
            // 添加参数 x-max-priority 以指定最大的优先级，值为0-255（整数）。
            $arguments['x-max-priority'] = $this->getQueueMaxPriority();
        }

        // 设置死信队列
        if ($this->isRerouteFailed()) {
            $arguments['x-dead-letter-exchange'] = $this->getFailedExchange() ?? '';
            $arguments['x-dead-letter-routing-key'] = $this->getFailedRoutingKey($destination);
        }

        return $arguments;
    }

    /**
     * Returns a integer with a default of '2' for when using prioritization on delayed messages.
     * If priority queues are desired, we recommend using between 1 and 10.
     * Using more priority layers, will consume more CPU resources and would affect runtimes.
     *
     * @see https://www.rabbitmq.com/priority.html
     * @return int
     */
    protected function getQueueMaxPriority(): int
    {
        return intval(Arr::get($this->options, 'queue_max_priority') ?: 2);
    }

    /**
     * Returns &true;, if failed messages should be rerouted.
     *
     * @return bool
     */
    protected function isRerouteFailed(): bool
    {
        return boolval(Arr::get($this->options, 'reroute_failed') ?: false);
    }

    /**
     * Get the exchange for failed messages.
     *
     * @param string|null $exchange
     * @return string|null
     */
    protected function getFailedExchange(string $exchange = null): ?string
    {
        return $exchange ?: Arr::get($this->options, 'failed_exchange') ?: null;
    }

    /**
     * Get the routing-key for failed messages
     * The default routing-key is the given destination substituted by '.failed'.
     *
     * @param string $destination
     * @return string
     */
    protected function getFailedRoutingKey(string $destination): string
    {
        return ltrim(sprintf(Arr::get($this->options, 'failed_routing_key') ?: '%s.failed', $destination), '.');
    }

    /**
     * Get the Delay queue arguments.
     *
     * @param string $destination
     * @param int $ttl
     * @return array
     */
    protected function getDelayQueueArguments(string $destination, int $ttl): array
    {
        return [
            'x-dead-letter-exchange' => $this->getExchange() ?? '',
            'x-dead-letter-routing-key' => $this->getRoutingKey($destination),
            'x-message-ttl' => $ttl, // 可以使用x-message-ttl参数设置当前队列中所有消息的过期时间，即当前队列中所有的消息过期时间都一样；
            'x-expires' => $ttl * 2, // 队列 $ttl * 2 秒没有被访问，就自动删除
        ];
    }

    /**
     * Acknowledge the message.
     *
     * @param RabbitMQJob $job
     * @return void
     */
    public function ack(RabbitMQJob $job): void
    {
        $this->channel->basic_ack($job->getRabbitMQMessage()->getDeliveryTag());
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }

    /**
     * @return AbstractConnection
     */
    public function getConnection(): AbstractConnection
    {
        return $this->connection;
    }

    /**
     * * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     * @throws RabbitMQException
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        foreach ((array) $jobs as $job) {
            $this->bulkRaw($this->createPayload($job, $queue, $data), $queue, ['job' => $job]);
        }

        $this->channel->publish_batch();
    }

    /**
     * @param string $payload
     * @param null $queue
     * @param array $options
     * @return mixed
     * @throws RabbitMQException|AMQPProtocolChannelException
     */
    public function bulkRaw(string $payload, $queue = null, array $options = [])
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        $this->channel->batch_basic_publish($message, $exchange, $destination);

        return $correlationId;
    }

    /**
     * Checks if the given exchange already present/defined in RabbitMQ.
     * Returns false when when the exchange is missing.
     *
     * @param string $exchange
     * @return bool
     * @throws AMQPProtocolChannelException
     */
    public function isExchangeExists(string $exchange): bool
    {
        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->connection->channel();
            $channel->exchange_declare($exchange, '', true);
            $channel->close();

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a exchange in rabbitMQ, when not already declared.
     *
     * @param string $name
     * @param string $type
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     * @return void
     */
    public function declareExchange(string $name, string $type = AMQPExchangeType::DIRECT, bool $durable = true, bool $autoDelete = false, array $arguments = []): void
    {
        if ($this->isExchangeDeclared($name)) {
            return;
        }

        $this->channel->exchange_declare(
            $name,
            $type,
            false,
            $durable,
            $autoDelete,
            false,
            true,
            new AMQPTable($arguments)
        );
    }

    /**
     * Delete a exchange from rabbitMQ, only when present in RabbitMQ.
     *
     * @param string $name
     * @param bool $unused
     * @return void
     * @throws AMQPProtocolChannelException
     */
    public function deleteExchange(string $name, bool $unused = false): void
    {
        if (! $this->isExchangeExists($name)) {
            return;
        }

        $this->channel->exchange_delete(
            $name,
            $unused
        );
    }

    /**
     * Delete a queue from rabbitMQ, only when present in RabbitMQ.
     *
     * @param string $name
     * @param bool $if_unused
     * @param bool $if_empty
     * @return void
     * @throws AMQPProtocolChannelException
     */
    public function deleteQueue(string $name, bool $if_unused = false, bool $if_empty = false): void
    {
        if (! $this->isQueueExists($name)) {
            return;
        }

        $this->channel->queue_delete($name, $if_unused, $if_empty);
    }

    /**
     * Bind a queue to an exchange.
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routingKey
     * @return void
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey = ''): void
    {
        if (in_array(
            implode('', compact('queue', 'exchange', 'routingKey')),
            $this->boundQueues,
            true
        )) {
            return;
        }

        $this->channel->queue_bind($queue, $exchange, $routingKey);
    }

    /**
     * Purge the queue of messages.
     *
     * @param string $queue
     * @return void
     */
    public function purge(string $queue = null): void
    {
        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->connection->channel();
        $channel->queue_purge($this->getQueue($queue));
        $channel->close();
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param object|string $job
     * @param string $queue
     * @param string $data
     * @return array
     */
    protected function createPayloadArray($job, $queue, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getRandomId(),
        ]);
    }

    /**
     * Get a random ID string.
     *
     * @return string
     */
    protected function getRandomId(): string
    {
        return Str::uuid();
    }

    /**
     * Checks if the exchange was already declared.
     *
     * @param string $name
     * @return bool
     */
    protected function isExchangeDeclared(string $name): bool
    {
        return in_array($name, $this->exchanges, true);
    }
}
