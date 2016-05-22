<?php
namespace ProcessMQ;

use Cake\Core\Configure;
use Cake\Core\Exception\Exception;
use Cake\Datasource\ConnectionManager;

/**
 * Queue utility class making working with RabbitMQ a lot easier
 *
 */
class Queue
{

    /**
     * Queue configuration read from the YAML file
     *
     * @var array
     */
    protected static $_config = [];

    /**
     * List of exchanges for publication
     *
     * @var \ProcessMQ\Connection\RabbitMQConnection[]
     */
    protected static $_publishers = [];

    /**
     * List of queues for consumption
     *
     * @var array
     */
    protected static $_consumers = [];

    /**
     * Get the queue object for consumption
     *
     * @param  string $name
     * @return \AMQPQueue
     * @throws \Exception on missing consumer configuration.
     */
    public static function consume($name)
    {
        $config = static::get($name);
        if (empty($config['consume'])) {
            throw new Exception('Missing consumer configuration (' . $name . ')');
        }

        $config = $config['consume'];
        $config += [
            'connection' => 'rabbit',
            'prefetchCount' => 1,
        ];

        if (!array_key_exists($name, static::$_consumers)) {
            $connection = ConnectionManager::get($config['connection']);
            static::$_consumers[$name] = $connection->queue($config['queue'], $config);
        }

        return static::$_consumers[$name];
    }

    /**
     * Publish a message to a RabbitMQ exchange
     *
     * @param  string $name
     * @param  mixed  $data
     * @param  array  $options
     * @return boolean
     */
    public static function publish($name, $data, array $options = [])
    {
        $config = static::get($name);
        if (empty($config['publish'])) {
            throw new Exception('Missing publisher configuration (' . $name . ')');
        }

        $config = $config['publish'];
        $config += $options;
        $config += [
            'connection' => 'rabbit',
            'className' => 'RabbitMQ.RabbitQueue'
        ];

        if (!array_key_exists($name, static::$_publishers)) {
            static::$_publishers[$name] = ConnectionManager::get($config['connection']);
        }

        return static::$_publishers[$name]->send($config['exchange'], $config['routing'], $data, $config);
    }

    /**
     * Test if a queue is configured
     *
     * @param  string $name
     * @return boolean
     */
    public static function configured($name)
    {
        static::_load();
        return array_key_exists($name, static::$_config);
    }

    /**
     * Get the queue configuration
     *
     * @param  string $name
     * @return array
     */
    public static function get($name)
    {
        if (!static::configured($name)) {
            throw new Exception([$name]);
        }

        return static::$_config[$name];
    }

    /**
     * Clear all internal state in the class
     *
     * @return void
     */
    public static function clear()
    {
        static::$_config = [];
        static::$_publishers = [];
        static::$_consumers = [];
    }

    /**
     * Load the configuration array
     *
     * @return void
     */
    protected static function _load()
    {
        if (!empty(static::$_config)) {
            return;
        }
        static::$_config = Configure::read('Queues') ?: [];
    }

    /**
     * Class is purely static and singleton
     */
    protected function __construct()
    {

    }
}
