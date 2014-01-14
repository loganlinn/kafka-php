<?php

/**
 * Kafka connection object.
 *
 * Currently connects to a single broker, it can be later on extended to provide
 * an auto-balanced connection to the cluster of borkers without disrupting the
 * client code.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */

namespace Kafka;

require_once 'Exception.php';
require_once 'Offset.php';
require_once 'Offset_32bit.php';
require_once 'Offset_64bit.php';
require_once 'Message.php';
require_once 'IConsumer.php';
require_once 'IMetadata.php';
require_once 'IProducer.php';
require_once 'TopicFilter.php';
require_once 'ConsumerConnector.php';
require_once 'ConsumerContext.php';
require_once 'ProducerConnector.php';
require_once 'MessageStream.php';
require_once 'Partitioner.php';

class Kafka
{
    const MAGIC_0 = 0; // wire format without compression attribute
    const MAGIC_1 = 1; // wire format with compression attribute

    const REQUEST_KEY_PRODUCE      = 0;
    const REQUEST_KEY_FETCH        = 1;
    const REQUEST_KEY_MULTIFETCH   = 2;
    const REQUEST_KEY_MULTIPRODUCE = 3;
    const REQUEST_KEY_OFFSETS      = 4;

    const COMPRESSION_NONE = 0;
    const COMPRESSION_GZIP = 1;
    const COMPRESSION_SNAPPY = 2;

    const OFFSETS_LATEST = -1;
    const OFFSETS_EARLIEST = -2;

    const CLIENT_ID = 'KAFKA-PHP';

    /* v 0.8 constant */
    const REQUEST_KEY_METADATA = 3;
    const REQUEST_API_VERSION = 0;

    const REQUEST_ACK_NONE = 0;
    const REQUEST_ACK_LEADER = 1;
    const REQUEST_ACK_ALL = -1;

    // connection properties
    private $connections;
    private $timeout;
    private $producerClass;
    private $consumerClass;
    private $clientId;

    /**
     * Constructor
     *
     * @param Array  $connections Kafka hosts ex ['localhost:9092']
     * @param int    $timeout socket timeout, in seconds
     * @param int    $kapiVersion Kafka API Version
     *     - the client currently recoginzes difference in the wire
     *    format prior to the version 0.8 and the versioned
     *    requests introduced in 0.8
     */
    public function __construct(
        $connections = "",
        $timeout = 6,
        $apiVersion = 0.8,
        $clientId = self::CLIENT_ID
    )
    {
        $this->connections = explode(",", $connections);
        $this->timeout = $timeout;
        $this->clientId = $clientId;
        $apiImplementation = self::getApiImplementation($apiVersion);
        include_once "{$apiImplementation}/ProducerChannel.php";
        $this->producerClass = "\Kafka\\$apiImplementation\ProducerChannel";
        include_once "{$apiImplementation}/ConsumerChannel.php";
        $this->consumerClass = "\Kafka\\$apiImplementation\ConsumerChannel";
    }

    /**
     * @param  float  $apiVersion
     * @return string
     */
    public static function getApiImplementation($apiVersion)
    {
        if ($apiVersion < 0.8) {
            $apiImplementation = "V07";
        } elseif ($apiVersion < 0.9) {
            $apiImplementation = "V08";
        } else {
            throw new \Kafka\Exception(
                "Unsupported Kafka API version $apiVersion"
            );
        }

        return $apiImplementation;
    }

    /**
     * @param Integer the connection number order default to 0
     * @throw \Kafka\Exception if num is greater than number of connections
     * @return string "protocol://<host>:<port>";
     */
    public function getConnectionString($num=0)
    {
        if ($num > count($this->connections)) {
            throw new \Kafka\Exception('Connection number exeeded number of broker');
        }
        return "tcp://{$this->connections[$num]}";
    }

    /**
     * @return Array
     */
    public function getConnection()
    {
        return $this->connections;
    }

    /**
     * @return int
     */
    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * @return IProducer
     */
    public function createProducer($requiredAcks = self::REQUEST_ACK_LEADER)
    {
        $producerClass = $this->producerClass;

        return new $producerClass($this, $requiredAcks);
    }

    /**
     * @return IConsumer
     */
    public function createConsumer()
    {
        $consumerClass = $this->consumerClass;

        return new $consumerClass($this);
    }
    public function getClientid() {
        return $this->clientId;
    }


}
