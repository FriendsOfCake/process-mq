<?php
namespace ProcessMQ\Connection;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use AMQPQueue;
use Cake\Database\Log\QueryLogger;
use RuntimeException;

/**
 * RabbitMQ Connection object
 *
 */
class RabbitMQConnection {

    /**
     * Configuration options
     *
     * @var array
     */
    public $_config = [
        'user' => 'guest',
        'password' => 'guest',
        'host' => 'localhost',
        'port' => '5672',
        'vhost' => '/',
        'timeout' => 0,
        'readTimeout' => 172800 // two days
    ];

    /**
     * Connection object
     *
     * @var AMQPConnection
     */
    protected $_connection;

    /**
     * List of queues
     *
     * @var array
     */
    protected $_queues = [];

    /**
     * List of exchanges
     *
     * @var array
     */
    protected $_exchanges = [];

    /**
     * List of channels
     *
     * @var array
     */
    protected $_channels = [];

    /**
     * Logger instance
     *
     * @var mixed
     */
    protected $_logger;

    /**
     * Whether or not to log queries.
     *
     * @var bool
     */
    protected $_logQueries;

    /**
     * Initializes connection to RabbitMQ
     */
    public function __construct($config = []) {
        $this->_config = $config + $this->_config;
        $this->_connection = new AMQPConnection();
        $this->connect();
    }

    /**
     * Get the configuration data used to create the connection.
     *
     * @return array
     */
    public function config() {
        return $this->_config;
    }

    /**
     * Get the configuration name for this connection.
     *
     * @return string
     */
    public function configName() {
        if (empty($this->_config['name'])) {
            return '';
        }

        return $this->_config['name'];
    }

    /**
     * Enables or disables query logging for this connection.
     *
     * @param bool $enable whether to turn logging on or disable it.
     *   Use null to read current value.
     * @return bool
     */
    public function logQueries($enable = null) {
        if ($enable !== null) {
            $this->_logQueries = $enable;
        }

        if ($this->_logQueries) {
            return $this->_logQueries;
        }

        return $this->_logQueries = $enable;
    }

    /**
     * Sets the logger object instance. When called with no arguments
     * it returns the currently setup logger instance.
     *
     * @param object $logger logger object instance
     * @return object logger instance
     */
    public function logger($logger = null) {
        if ($logger) {
            $this->_logger = $logger;
        }

        if ($this->_logger) {
            return $this->_logger;
        }

        $this->_logger = new QueryLogger();
        return $this->_logger;
    }

    /**
     * Connects to RabbitMQ
     *
     * @return void
     */
    public function connect() {
        $connection = $this->_connection;
        $connection->setLogin($this->_config['user']);
        $connection->setPassword($this->_config['password']);
        $connection->setHost($this->_config['host']);
        $connection->setPort($this->_config['port']);
        $connection->setVhost($this->_config['vhost']);
        $connection->setReadTimeout($this->_config['readTimeout']);

        # You shall not use persistent connections
        #
        # AMQPChannelException' with message 'Could not create channel. Connection has no open channel slots remaining
        #
        # The PECL extension is foobar, http://stackoverflow.com/questions/23864647/how-to-avoid-max-channels-per-tcp-connection-with-amqp-php-persistent-connectio
        #

        $connection->connect();
    }

    /**
     * Returns the internal connection object
     *
     * @return AMQPConnection
     */
    public function connection() {
        return $this->_connection;
    }

    /**
     * Creates a new channel to communicate with an exchange or queue object
     *
     * @param array $options
     * @return AMQPChannel
     */
    public function channel($name, $options = []) {
        if (empty($this->_channels[$name])) {
            $this->_channels[$name] = new AMQPChannel($this->connection());
            if (!empty($options['prefetchCount'])) {
                $this->_channels[$name]->setPrefetchCount((int)$options['prefetchCount']);
            }
        }

        return $this->_channels[$name];
    }

    /**
     * Connects to an exchange with the given name, the object returned
     * can be used to configure and create a new one.
     *
     * @param string $name
     * @param array $options
     * @return AMQPExchange
     */
    public function exchange($name, $options = []) {
        if (empty($this->_exchanges[$name])) {
            $channel = $this->channel($name, $options);
            $exchange = new AMQPExchange($channel);
            $exchange->setName($name);

            if (!empty($options['type'])) {
                $exchange->setType($options['type']);
            }

            if (!empty($options['flags'])) {
                $exchange->setFlags($options['flags']);
            }

            $this->_exchanges[$name] = $exchange;
        }

        return $this->_exchanges[$name];
    }

    /**
     * Connects to a queue with the given name, the object returned
     * can be used to configure and create a new one.
     *
     * @param string $name
     * @param array $options
     * @return AMQPQueue
     */
    public function queue($name, $options = []) {
        if (empty($this->_queues[$name])) {
            $channel = $this->channel($name, $options);
            $queue = new AMQPQueue($channel);
            $queue->setName($name);

            if (!empty($options['flags'])) {
                $queue->setFlags($options['flags']);
            }

            $this->_queues[$name] = $queue;
        }

        return $this->_queues[$name];
    }

    /**
     * Creates a new message in the exchange $topic with routing key $task, containing
     * $message
     *
     * @param string $topic
     * @param string $task
     * @param mixed $data
     * @param array $options
     * @return boolean
     */
    public function send($topic, $task, $data, array $options = []) {
        list($data, $attributes, $options) = $this->_prepareMessage($data, $options);
        return $this->exchange($topic)->publish($data, $task, AMQP_NOPARAM, $attributes);
    }

    /**
     * Creates a list of new $messages in the exchange $topic with routing key $task
     *
     * @param string $topic
     * @param string $task
     * @param array $messages
     * @param array $options
     * @return boolean
     */
    public function sendBatch($topic, $task, array $messages, array $options = []) {
        return array_walk($messages, function ($data) use ($topic, $task, $options) {
            $this->send($topic, $task, $data, $options);
        });
    }


    /**
     * Prepare a message by serializing, optionally compressing and setting the correct content type
     * and content type for the message going to RabbitMQ
     *
     * @param  mixed $data
     * @param  array $options
     * @return array
     */
    protected function _prepareMessage($data, array $options) {
        $attributes = [];

        $options += [
            'silent' => false,
            'compress' => true,
            'serializer' => extension_loaded('msgpack') ? 'msgpack' : 'json',
            'delivery_mode' => 1
        ];

        switch ($options['serializer']) {
            case 'json':
                $data = json_encode($data);
                $attributes['content_type'] = 'application/json';
                break;

            case 'text':
                $attributes['content_type'] = 'application/text';
                break;

            default:
                throw new RuntimeException('Unknown serializer: ' . $options['serializer']);
        }

        if (!empty($options['compress'])) {
            $data = gzcompress($data);
            $attributes += ['content_encoding' => 'gzip'];
        }

        if (!empty($options['delivery_mode'])) {
            $attributes['delivery_mode'] = $options['delivery_mode'];
        }

        return [$data, $attributes, $options];
    }

}
