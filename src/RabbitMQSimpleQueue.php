<?php


namespace LaravelRabbitMQ;


use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exchange\AMQPExchangeType;

class RabbitMQSimpleQueue extends RabbitMQQueue
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
     * @return RabbitMQQueue
     * @throws AMQPProtocolChannelException
     * @throws Exceptions\RabbitMQException
     */
    public function configurationConsume($queue): RabbitMQQueue
    {
        // 声明queue
        [$destination, $exchange, $exchangeType, ] = $this->publishProperties($queue, []);
        $this->declareDestination($destination, $exchange, $exchangeType);

        return $this;
    }
}
