<?php

require_once __DIR__ . "/../../../src/Kafka/Kafka.php";
require_once __DIR__ . "/../../../src/Kafka/V08/Channel.php";
use Kafka\Kafka;
use Kafka\Message;

class TestChannel08 extends \Kafka\V08\Channel
{
    public function __construct() {}
    public function setStreamContents($contents)
    {
        rewind($this->socket); fwrite($this->socket, $contents); rewind($this->socket);
    }
    public function getStreamContents()
    {
        rewind($this->socket); return stream_get_contents($this->socket);
    }
    protected function createSocket()
    {
        if (!is_resource($this->socket)) { $this->socket = fopen("php://memory", "rw");}
    }
    public function packMessage2(Message $message, $overrideCompression = null)
    {
        return $this->packMessage($message, $overrideCompression);
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

//test uncompressed single message with no key
$channel = new TestChannel08();
$message1 = new Message("topic1", 2, "Hello!", Kafka::COMPRESSION_NONE);
$data = $channel->packMessage2($message1);

$expectation = chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0); // 8 byte message offset
$expectation .= chr(0).chr(0).chr(0).chr(20); // 4 byte size of the next payload
$expectation .= chr(141).chr(199).chr(149).chr(162); // 4 byte CRC
$expectation .= chr(0); // 1 byte magic
$expectation .= chr(0); // 1 byte attribute
$expectation .= chr(255).chr(255).chr(255).chr(255); // 4 byte KEY (-1 means no key)
$expectation .= chr(0).chr(0).chr(0).chr(6); // 4 byte message length
$expectation .= chr(72).chr(101).chr(108).chr(108).chr(111).chr(33); // 6 byte message 'Hello!'
assert($data === $expectation);


// test uncompressed single message with key 'foo'
$channel = new TestChannel08();
$message1 = new Message("topic1", 2, "Hello!", Kafka::COMPRESSION_NONE, null, 'foo');
$data = $channel->packMessage2($message1);

$expectation = chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0); // 8 byte message offset
$expectation .= chr(0).chr(0).chr(0).chr(23); // 4 byte size of the next payload
$expectation .= chr(222).chr(173).chr(32).chr(127); // 4 byte CRC
$expectation .= chr(0); // 1 byte magic
$expectation .= chr(0); // 1 byte attribute
$expectation .= chr(0).chr(0).chr(0).chr(3); // 4 byte KEY (-1 means no key)
$expectation .= chr(102).chr(111).chr(111); // 6 byte message 'Hello!'
$expectation .= chr(0).chr(0).chr(0).chr(6); // 4 byte message length
$expectation .= chr(72).chr(101).chr(108).chr(108).chr(111).chr(33); // 6 byte message 'Hello!'

assert($data === $expectation);


//test gzip compressed single message. Not Implemented yet!
/*
$channel = new TestChannel08();
$message2 = new Message("topic1", 2, "Hello!", Kafka::COMPRESSION_GZIP);
$data = $channel->packMessage2($message2);
assert($data === chr(0).chr(0).chr(0).chr(32).chr(1).chr(1).chr(90).chr(49).chr(84).chr(139).chr(31).chr(139).chr(8).chr(0).chr(0).chr(0).chr(0).chr(0).chr(0).chr(3).chr(243).chr(72).chr(205).chr(201).chr(201).chr(87).chr(4).chr(0).chr(86).chr(204).chr(42).chr(157).chr(6).chr(0).chr(0).chr(0));
 */

//test send request 0.8 wire format and can read it back
$channel = new TestChannel08();
$requestData = "xyz-request-data";
$channel->send2($requestData, true); //expect response
$data = $channel->getStreamContents();
assert(strlen($data) === strlen($requestData) + 4);
assert($data === chr(0).chr(0).chr(0).chr(strlen($requestData)).$requestData);

// we don't perform response error here beacause error code are inside of the Response message now// not header
