<?php


namespace LaravelRabbitMQ\Connectors;

use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Arr;
use Exception;
use Illuminate\Queue\Events\WorkerStopping;
use InvalidArgumentException;
use Illuminate\Events\Dispatcher;
use LaravelRabbitMQ\RabbitMQQueue;
use LaravelRabbitMQ\RabbitMQWorkerQueue;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class RabbitMQConnector implements ConnectorInterface
{
    /**
     * @var Dispatcher
     */
    private $dispatcher;

    public function __construct(Dispatcher $dispatcher)
    {
        $this->dispatcher = $dispatcher;
    }

    /**
     * @inheritDoc
     * @throws Exception
     */
    public function connect(array $config) : RabbitMQQueue
    {
        $connection = $this->createConnection(Arr::except($config, 'options.queue'));
        $queue = $this->createQueue(
            Arr::get($config, 'worker', 'default'),
            $connection,
            $config
        );

        if (! $queue instanceof RabbitMQQueue) {
            throw new InvalidArgumentException('Invalid worker.');
        }

        $this->dispatcher->listen(WorkerStopping::class, static function () use ($queue): void {
            $queue->close();
        });

        return $queue;
    }

    /**
     * @param array $config
     * @return AbstractConnection
     * @throws Exception
     */
    protected function createConnection(array $config): AbstractConnection
    {
        /** @var AbstractConnection $connection */
        $connection = Arr::get($config, 'connection', AMQPStreamConnection::class);

        // disable heartbeat when not configured, so long-running tasks will not fail
        $config = Arr::add($config, 'options.heartbeat', 0);

        // 获取第一个可用链接
        return $connection::create_connection(
            Arr::shuffle(Arr::get($config, 'hosts', [])), // 打乱rabbitmq hosts list
            $this->filter(Arr::get($config, 'options', []))
        );
    }

    /**
     * Create a queue for the worker.
     *
     * @param string $worker
     * @param AbstractConnection $connection
     * @param array $config
     * @return RabbitMQWorkerQueue|Queue
     */
    protected function createQueue(string $worker, AbstractConnection $connection, $config)
    {
        $queue = $config['queue'];
        $options = Arr::get($config, 'options.queue', []);
        switch ($worker) {
            case 'default':
                return new RabbitMQWorkerQueue($connection, $queue, $options, $config);
            default:
                return new $worker($connection, $queue, $options, $config);
        }
    }

    /**
     * Recursively filter only null values.
     *
     * @param array $array
     * @return array
     */
    private function filter(array $array): array
    {
        foreach ($array as $index => &$value) {
            if (is_array($value)) {
                $value = $this->filter($value);
                continue;
            }

            // If the value is null then remove it.
            if ($value === null) {
                unset($array[$index]);
                continue;
            }
        }

        return $array;
    }
}
