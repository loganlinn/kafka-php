<?php

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";
require_once __DIR__ . "/../../../src/Kafka/V08/ProducerChannel.php";

use Kafka\Kafka;
use Kafka\Message;


define('TEST_HOST', 'localhost'); // fake broker, no need to actually run kafka
define('TEST_PORT', '9292');

class TestV08ProducerChannel extends \Kafka\V08\ProducerChannel
{
    public function setMetadata2(\Kafka\V08\Metadata $metadata)
    {
        $this->setMetadata($metadata);
    }

    public function setChannels2(Array $channels)
    {
        $this->setChannels($channels);
    }
}

class StubChannel extends \Kafka\V08\Channel
{
    public function __construct()
    {
        // default constructor
        parent::__construct('', 6, 'KAFKA-PHP', \Kafka\Kafka::REQUEST_ACK_NONE);
    }

    public function getStreamContents()
    {
        $this->createSocket();
        rewind($this->socket); return stream_get_contents($this->socket);
    }

    protected function createSocket()
    {
        if (!is_resource($this->socket)) $this->socket = fopen("php://memory", "rw");
    }

    public function strToHex2($string)
    {
        return $this->strToHex($string);
    }
}

// This class stub implementation of the metadata class
class StubMetadata extends \Kafka\V08\Metadata
{
    // override getTopicMetadata functions
    public function getTopicMetadata($topic = null)
    {
        $metadata = array(
            $topic => array(
                'error_code' => 0,
                'partitions' => array(
                    0 => array(
                        'error_code' => 0,
                        'leader_id' => 0,
                        'replicas' => array(0),
                        'isr' => array(0)
                    ),
                    1 => array(
                        'error_code' => 0,
                        'leader_id' => 0,
                        'replicas' => array(0),
                        'isr' => array(0)
                    ),
                    2 => array(
                        'error_code' => 0,
                        'leader_id' => 0,
                        'replicas' => array(0),
                        'isr' => array(0)
                    )
                )
            )
        );

        return $metadata;
    }

    public function getBrokerMetadata()
    {
        $metadata = array(
            0 => array(
                'host' => TEST_HOST,
                'port' => TEST_PORT,
            )
        );

        return $metadata;
    }

}


//test single message for multiple partitions of one topic without compression
$producer = new TestV08ProducerChannel(new Kafka(), \Kafka\Kafka::REQUEST_ACK_NONE);
$producer->setMetadata2(new StubMetadata(null));

// stub broker channel implementation 'node_id' => Channel
$channel = new StubChannel();
$key = TEST_HOST .':' . TEST_PORT;
$channels = array(
    $key => $channel,
);

$producer->setChannels2($channels);

$producer->add(new Message("topic1", 0, "Hello World 1!", Kafka::COMPRESSION_NONE));
$producer->add(new Message("topic1", 1, "Hello World 2!", Kafka::COMPRESSION_NONE));
$producer->add(new Message("topic1", 2, "Hello World 3!", Kafka::COMPRESSION_NONE));
$producer->produce();


$response = $channel->getStreamContents();

// expected 3 package of message

function produceMessage($correlationId, $partition, $message) {

    //first message
    $expected1 = pack('n', 0); // 2 byte API produce key (0)
    $expected1 .= pack('n', 0); // 2 byte API version (0)
    $expected1 .= pack('N', $correlationId); // correlationID
    $expected1 .= pack('n', 9); // 'KAFKA-PHP' string length
    $expected1 .= 'KAFKA-PHP'; // clientId
    $expected1 .= pack('n', \Kafka\Kafka::REQUEST_ACK_NONE); // 2 byte require none acks (0)
    $expected1 .= pack('N', 1000); // 4 byte kafka timeout im msec (1000)
    $expected1 .= pack('N', 1); // 4 byte number of following topic (1)
    $expected1 .= pack('n', 6); // 2 byte 'topic1' string length
    $expected1 .= 'topic1'; // topicname
    $expected1 .= pack('N', 1); // 4 byte number of following partition (1)
    $expected1 .= pack('N', $partition); // 4 byte partition numner (0)

    $messageSet = pack('NN', 0, 0); // 8 offset

    $message1 = chr(0); // 1 byte magicByte
    $message1 .= chr(0); // 1 byte Attributes
    $message1 .= chr(255).chr(255).chr(255).chr(255); // 4 byte key length 0xFFFFFFFF (-1) for null
    $message1 .= pack('N', strlen($message)); // 4 byte length "Hello World1!"
    $message1 .= $message;

    $message1 = pack('N', crc32($message1)) . $message1; // 4 byte crc infront

    $message1 = pack('N', strlen($message1)) . $message1; //add message size infront
    $messageSet .= $message1;

    $messageSet = pack('N', strlen($messageSet)) . $messageSet; // add messageSet size infront

    $expected1 .= $messageSet;
    //add size in front
    $expected1 = pack('N', strlen($expected1)) . $expected1;

    return $expected1;
}

$expected1 = produceMessage(1, 0, "Hello World 1!");
$expected2 = produceMessage(2, 1, "Hello World 2!");
$expected3 = produceMessage(3, 2, "Hello World 3!");

$length = strlen($expected1);
$responsePart1 = substr($response, 0, $length);
$responsePart2 = substr($response, $length, $length);
$responsePart3 = substr($response, 2 * $length, $length);

assert($expected1 === $responsePart1);
assert($expected2 === $responsePart2);
assert($expected3 === $responsePart3);



