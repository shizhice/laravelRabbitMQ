<?php


namespace LaravelRabbitMQ;


use LaravelRabbitMQ\Exceptions\RabbitMQException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exchange\AMQPExchangeType;

class RabbitMQWorkerQueue extends RabbitMQQueue
{
    /**
     * @inheritDoc
     * @throws AMQPProtocolChannelException
     */
    protected function declareDestination(string $destination, ?string $exchange = null, string $exchangeType = AMQPExchangeType::DIRECT): void
    {
        // When the queue already exists, just return.
        if ($this->isQueueExists($destination)) {
            return;
        }

        // Create a queue for amq.direct publishing.
        $this->declareQueue($destination, true, false, $this->getQueueArguments($destination));
    }

    /**
     * @inheritDoc
     * @throws AMQPProtocolChannelException
     * @throws RabbitMQException
     */
    public function configurationConsume($queue): RabbitMQQueue
    {
        // 声明queue
        [$destination, $exchange, $exchangeType, ] = $this->publishProperties($queue, []);
        $this->declareDestination($destination, $exchange, $exchangeType);

        // 流量控制
        $prefetchCount = intval($this->config["prefetchCount"] ?? 1);
        $this->channel->basic_qos(
            null,
            max($prefetchCount, 1),
            null
        );
        return $this;
    }
}
