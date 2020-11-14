<?php

namespace LaravelRabbitMQ\Console;

use LaravelRabbitMQ\Jobs\RabbitMqTestJob1;
use LaravelRabbitMQ\Jobs\RabbitMqTestJob2;
use LaravelRabbitMQ\Jobs\RabbitMqTestJob3;
use LaravelRabbitMQ\Jobs\RabbitMqTestJob4;
use Illuminate\Console\Command;

class TestRabbitMQ extends Command
{
    const MODE_OPTIONS = [
        "simple" => "rabbitmq-simple",
        "worker" => "rabbitmq-worker",
        "pub-sub" => "rabbitmq-pub-sub",
        "topic" => "rabbitmq-topic",
        "headers" => "rabbitmq-headers",
        "rpc" => "rabbitmq-rpc",
    ];

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rabbitmq:test
                            {--mode= : default worker, options: simple、worker、pub-sub、topic、headers、rpc}
                            {--num= : push num}
                            {--queue= : queue name}
                            ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $mode = $this->getMode();
        $num = $this->getPushNum();
        $queue = $this->getQueue();

        $count = 0;
        while ($num > 0) {
            $num--;
            $data = [
                "serial_number" => sprintf("#%d", ++$count),
                "msg" => "Hello RabbitMQ."
            ];
            $job = RabbitMqTestJob1::withChain([
                (new RabbitMqTestJob2("%% $count.1"))->onConnection(self::MODE_OPTIONS[$mode])->onQueue($queue),
                (new RabbitMqTestJob3("%% $count.2"))->onConnection(self::MODE_OPTIONS[$mode])->onQueue($queue),
                (new RabbitMqTestJob4("%% $count.3"))->onConnection(self::MODE_OPTIONS[$mode])->onQueue($queue)
            ])->dispatch($data)->onConnection(self::MODE_OPTIONS[$mode])->onQueue($queue);
        }
    }

    private function getMode()
    {
        $mode = $this->option("mode");

        return $mode && array_key_exists($mode, self::MODE_OPTIONS) ? $mode : 'worker';
    }

    private function getPushNum()
    {
        return max(intval($this->option("num")), 1);
    }

    private function getQueue()
    {
        return $this->option("queue") ?: null;
    }
}
