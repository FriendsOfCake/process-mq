<?php
namespace ProcessMQ\Shell\Task;

use AMQPEnvelope;
use AMQPQueue;
use Cake\Console\Shell;
use Cake\Event\EventDispatcherTrait;
use Exception;
use ProcessMQ\Queue;
use RuntimeException;

/**
 * Utility functions to setup a worker server
 *
 * @property \ProcessManager\Shell\Task\ProcessManagerTask ProcessManager
 */
class RabbitMQWorkerTask extends Shell
{

    use EventDispatcherTrait;

    /**
     * List of tasks to use
     *
     * @var array
     */
    public $tasks = ['ProcessManager.ProcessManager'];

    /**
     * Changed by the signal handler
     *
     * @var boolean
     */
    public $stop = false;

    /**
     * Are the worker busy?
     *
     * @var boolean
     */
    public $_working = false;

    /**
     * Consume a queue by the RabbitMQ queue configuration
     *
     * @param string $config
     * @param callable $callable
     * @return void
     */
    public function consume($config, callable $callable)
    {
        $this->ProcessManager->handleKillSignals();
        $this->eventManager()->attach([$this, 'signalHandler'], 'CLI.signal', ['priority' => 100]);

        $this->_consume(Queue::consume($config), $callable);
    }

    /**
     * Executes the $callable method by passing the queued messages one at a time.
     * If the callable returns false or throws an exception, the message will be re-queued
     *
     * @param \AMQPQueue $queue
     * @param callable $callable
     * @return void
     */
    protected function _consume(AMQPQueue $queue, callable $callable)
    {
        $tag = uniqid() . microtime(true);

        $callback = function (AMQPEnvelope $envelope, AMQPQueue $queue) use ($callable) {
            return $this->_callback($callable, $envelope, $queue);
        };

        $queue->consume($callback, AMQP_NOPARAM, $tag);
    }

    /**
     * Actual callback method
     *
     * @param  callable $callable
     * @param \AMQPEnvelope $envelope
     * @param \AMQPQueue $queue
     * @return boolean
     * @throws \Exception
     * @throws \RuntimeException
     */
    protected function _callback(callable $callable, AMQPEnvelope $envelope, AMQPQueue $queue)
    {
        if ($this->stop) {
            return false;
        }

        if ($envelope === false) {
            return false;
        }

        if ($this->stop) {
            $this->log('-> Putting job back into the queue', 'warning');
            $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);
            return false;
        }

        $result = false;

        try {
            $this->_working = true;

            $body = $envelope->getBody();
            $compressed = $envelope->getContentEncoding() === 'gzip';

            if ($compressed) {
                $body = gzuncompress($body);
            }

            switch ($envelope->getContentType()) {
                case 'application/json':
                    $result = $callable(json_decode($body, true), $envelope, $queue);
                    break;

                case 'application/text':
                    $result = $callable($body, $envelope, $queue);
                    break;

                default:
                    throw new RuntimeException('Unknown serializer: ' . $envelope->getContentType());
            }

        } catch (Exception $e) {
            $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);
            throw $e;
        } finally {
            $this->_working = false;
        }

        if ($result !== false) {
            return $queue->ack($envelope->getDeliveryTag());
        }

        return $queue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE);
    }

    /**
     * Handle signals from the OS
     *
     * @return void
     */
    public function signalHandler()
    {
        if (!$this->_working) {
            $this->log('Not doing any jobs, going to die now...', 'warning');
            $this->_stop();
            return;
        }

        $this->stop = true;
    }
}
