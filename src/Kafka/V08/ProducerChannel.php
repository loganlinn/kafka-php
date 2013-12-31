<?php
/**
 * Kafka 0.7 Producer Channel.
 *
 * In this version there is no acknowledgement that the message has
 * been received by the broker so the success is measured only
 * by writing succesfully into the socket.
 *
 * The channel implementation, however is the same as in 0.7
 *
 * @author michal.harish@gmail.com
 */

namespace Kafka\V08;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\Iproducer;

require_once realpath(dirname(__FILE__) . '/Channel.php');
require_once realpath(dirname(__FILE__) . '/Metadata.php');

class ProducerChannel implements IProducer
{
    /**
     * @var array
     */
    private $messageQueue;

    /**
     * Required acknowledgement from brokker
     * * This field indicates how many acknowledgements the servers * should receive before responding to the requet:
     * 0 -> dont wait for acks
     * 1 -> wait until leader commit the message
     * n -> n > 1, wait for n broker to commit
     * -1 -> wait untill all replcia commit the message
     * @var Integer
     */
    private $requiredAcks;

    private $ackTimeout = 1000; // 1 sec

    private $connection;

    /**
     * @var Array mapping betweet 'host:port' to a channel
     */
    private $channels;

    /**
     * @var Array mapping between broker_id to a channel
     */
    private $brokers;

    private $metadata;

    /**
     * @param Kafka $connection
     * @param $requiredAcks @see requiredAcks
     */
    public function __construct(Kafka $connection, $requiredAcks = \Kafka\Kafka::REQUEST_ACK_NONE)
    {
        $this->connection = $connection;
        $this->messageQueue = array();
        $this->channels = array();
        $this->requiredAcks = $requiredAcks;

        $this->channels = array();
        $connections = $connection->getConnection();

        if (count($connections) == 0) {
            throw new \Kafka\Exception("Empty Brooker list");
        }

        for ($i=0; $i<count($connections); $i++) {
            $hostPort = $connections[$i];
            $this->channels[$hostPort] = new Channel(
                $hostPort,
                $connection->getTimeout(),
                $connection->getClientid(),
                $this->requiredAcks
            );
        }

        // get first element, maybe we pass multiple broker in the future to allow retry to multiple broker ?
        $this->metadata = new Metadata(reset($this->channels));
    }

    /**
     * Set metadata implementation
     *
     * This function used to inject metadata class to producer. currently used for testing
     */
    protected function setMetadata(\Kafka\V08\Metadata $metadata)
    {
        $this->metadata = $metadata;
    }

    /**
     * Set array of ('node_id' => \Kafka\V08\Channel)
     *
     * This function used to inject brokers dependency to producer. currently used for testing
     */
    protected function setChannels(Array $channels)
    {
        $this->channels = $channels;
    }

    /**
     * Add a single message to the produce queue.
     *
     * @param  Message|string $message
     * @return boolean        Success
     */
    public function add(Message $message)
    {
        $this->messageQueue
            [$message->topic()]
            [$message->partition()]
            [] = $message;
    }


    /**
     * Produce all messages added.
     * @throws \Kafka\Exception On Failure
     * @return TRUE             On Success
     */
    public function produce()
    {

        if (count($this->messageQueue) == 0) {
            return true;
        }

        foreach ($this->messageQueue as $topic => &$partitions) {
            $metadata = $this->metadata->getTopicMetadata($topic);
            $topicMetadata = $metadata[$topic];

            // update node_id => channel mapping
            $brookerMetadata = $this->metadata->getBrokerMetadata();
            $this->updateNodeIdToChannel($brookerMetadata);

            foreach ($partitions as $partition => &$messageSet) {
                // get leader brooker
                $leaderId = @$topicMetadata['partitions'][$partition]['leader_id'];
                if ($leaderId === null) {
                    throw new \Kafka\Exception(
                        "Could not find metadata information of topic '{$topic}' partition '{$partition}'"
                    );
                }
                $leader = $this->brookers[$leaderId];
                $data = $leader->encodeProduceMessageSet($topic, $partition, $messageSet);

                $expectsResposne = $this->requiredAcks > 0;
                if ($leader->send($data, $expectsResposne)) {
                    if ($expectsResposne) {
                        $response = $leader->loadProduceResponse();
                        $errorCode = $response[$topic][$partition]['error_code'];
                        if ($errorCode != 0) {
                            throw \Kafka\Exception::createResponseException($errorCode);
                        }
                    }
                    unset($partitions[$partition]);
                }
                unset($messageSet);
            }
            unset($partitions);
        }

        return true;
    }

    public function close()
    {
        foreach($this->channels as $channel) {
            $channel->close();
        }
    }

    private function updateNodeIdToChannel($brookerMetadata)
    {
        foreach($brookerMetadata as $nodeId => $metadata) {
            $hostPort = $metadata['host'] . ':' . $metadata['port'];
            $channel = @$this->channels[$hostPort];

            if ($channel == null) {
                $channel = new \Kafka\V08\Channel(
                    $hostPort,
                    $this->connection->getTimeout(),
                    $this->connection->getClientid(),
                    $this->requiredAcks
                );
                $this->channels[$hostPort] = $channel;
            }

            $this->brookers[$nodeId] = $channel;
        }
    }
}
