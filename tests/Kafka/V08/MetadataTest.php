<?php

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";
require_once __DIR__ . "/../../../src/Kafka/V08/Channel.php";
require_once __DIR__ . "/../../../src/Kafka/V08/Metadata.php";
use Kafka\Kafka;
use Kafka\Message;
use Kafka\V08\Metadata;

class TestChannel08Metadata extends \Kafka\V08\Channel
{
    public function __construct() {}
    public function setStreamContents($contents)
    {
        rewind($this->socket); fwrite($this->socket, $contents); rewind($this->socket);
    }
    public function appendStreamContents($contents)
    {
        fwrite($this->socket, $contents);

        fseek($this->socket, -1 * strlen($contents), SEEK_END);
    }
    public function getStreamContents()
    {
        rewind($this->socket); return stream_get_contents($this->socket);
    }
    public function createSocket()
    {
        if (!is_resource($this->socket)) { $this->socket = fopen("php://memory", "rw");}
    }
    public function ftell() {
        return ftell($this->socket);
    }
    public function send2($requestData, $expectsResposne = true)
    {
        return $this->send($requestData, $expectsResposne);
    }
    public function read2($size, $stream = null)
    {
        return $this->read($size, $stream);
    }
    public function hasIncomingData2()
    {
        return $this->hasIncomingData();
    }

    public function strToHex2($data) {
        return $this->strToHex($data);
    }
}

class TestMetadata extends \kafka\V08\Metadata
{
    public function loadMetadataResponse2()
    {
        return $this->loadMetadataResponse();
    }
}

//test send request 0.8 wire format and can read it back
$channel = new TestChannel08Metadata();
$requestData = "xyz-request-data";
$channel->send2($requestData, true); //expect response
$data = $channel->getStreamContents();
assert(strlen($data) === strlen($requestData) + 4);
assert($data === chr(0).chr(0).chr(0).chr(strlen($requestData)).$requestData);



// Test getTopicMetadata
$brokerHost = 'test';
$brokerPort = 1;
$brokerId = 1;
$topicError = 0;
$topicName = 'test';
$partitionError = 0;
$partitionId = 1;
$leaderId = 1;
$replicaId = 8;
$isrId = 9;

$channel = new TestChannel08Metadata();

// setup response
$response = chr(0).chr(0).chr(0).chr(1); // add correlation ID;

// brooker informations
$response .= chr(0).chr(0).chr(0).chr(1); // 4 byte number of brokers
$response .= pack('N', $brokerId); // 4 byte broker id
$response .= pack('n', strlen($brokerHost)); // 2 byte string length broker name of 'TEST' (4)
$response .= $brokerHost; // message test in byte
$response .= pack('N', $brokerPort); // 4 byte port (1)

// topic information
$response .= chr(0).chr(0).chr(0).chr(1); // 4 byte number of topic
$response .= pack('n', $topicError); // 2 byte topic error
$response .= pack('n', strlen($topicName)); // 2 byte string length topic name of 'test' (4)
$response .= $topicName;
$response .= chr(0).chr(0).chr(0).chr(1); // 4 byte number of partition
$response .= pack('n', $partitionError); // 2 byte partition error
$response .= pack('N', $partitionId); // 4 byte partition ID
$response .= pack('N', $leaderId); // 4 byte leader ID
$response .= chr(0).chr(0).chr(0).chr(1); // 4 byte number of replica
$response .= pack('N', $replicaId); // 4 byte replica node id
$response .= chr(0).chr(0).chr(0).chr(1); // 4 byte number of isr
$response .= pack('N', $isrId); // 4 byte isr node id

$response = chr(0).chr(0).chr(0).chr(strlen($response)) . $response; // append 4 byte size


$channel->createSocket();
$channel->send('', true); // make the socket readable
$channel->appendStreamContents($response);

$metadata = new TestMetadata($channel);
$result = $metadata->loadMetadataResponse2();

assert(count($result['brokers']) === 1);
assert($result['brokers'][$brokerId]['host'] === $brokerHost);
assert($result['topics'][$topicName]['error_code'] === $topicError);

$partitionMetaData = $result['topics'][$topicName]['partitions'];
assert(count($partitionMetaData) === 1);
assert($partitionMetaData[$partitionId]['error_code'] === $partitionError);
assert($partitionMetaData[$partitionId]['leader_id'] === $leaderId);

$replicas = $partitionMetaData[$partitionId]['replicas'];
assert(count($replicas) === 1);
assert($replicas[0] === $replicaId);

$replicas = $partitionMetaData[$partitionId]['isr'];
assert(count($replicas) === 1);
assert($replicas[0] === $isrId);

