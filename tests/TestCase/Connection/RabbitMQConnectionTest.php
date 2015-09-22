<?php
namespace ProcessMQ\Test\TestCase\Connection;

use ProcessMQ\Connection\RabbitMQConnection;

class RabbitMQConnectionTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \ProcessMQ\Connection\RabbitMQConnection
     */
    public $connection;

    public function setUp()
    {
        $this->connection = new RabbitMQConnection();
    }

    public function tearDown()
    {
        unset($this->connection);
    }

    public function testConnect()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testChannel()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testExchange()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testQueue()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testSend()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testSendBatch()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }

    public function testPrepareMessage()
    {
        $this->markTestIncomplete('Not implemented yet.');
    }
}
