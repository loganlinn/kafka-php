<?php

/**
 * Channel
 *
 * Generic Kafka 0.8 response-request channel.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @author    Ahmy yulrizka <yulrizka@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @date      2013-11-13
 */

namespace Kafka\V08;

use Kafka\Offset;
use Kafka\Kafka;
use Kafka\Message;

abstract class Channel
{
    /**
     * Connection
     *
     * Connection object.
     *
     * @var Kafka
     */
    private $connection;

    /**
     * Socket
     *
     * Connection socket.
     *
     * @var Resource
     */
    protected $socket = null;

    /**
     * Socked send retry
     *
     * Number of times, a send operation should be attempted
     * when socket failure occurs.
     *
     * @var Integer
     */
    private $socketSendRetry = 1;

    /**
     * Rendable
     *
     * Request channel state.
     *
     * @var Boolean
     */
    private $readable;

    /**
     * Resnpose size
     *
     * Response of a readable channel.
     *
     * @var Integer
     */
    private $responseSize;

    /**
     * Read byets
     *
     * Number of bytes read from response.
     *
     * @var Integer
     */
    private $readBytes;

    /**
     * Inner stream
     *
     * Internal messageBatch for compressed sets of messages.
     *
     * @var Resource
     */
    private $innerStream = null;

    /**
     * Inner offset
     *
     * Internal for keeping the initial offset at which the innerStream starts
     * within the kafka stream.
     *
     * @var Offset
     */
    private $innerOffset = null;

    /**
     * Generated correleationId to identify every request
     * @var Integer
     */
    private $_correlationId = 0;

    /**
     * list of kafka brookers information returned by a brooker
     * @var Array
     */
    private $brokerList = array();

    /**
     * Constructor
     *
     * @param Kafka   $connection
     * @param String  $topic
     * @param Integer $partition
     */
    public function __construct(\Kafka\Kafka $connection)
    {
        $this->connection = $connection;
        $this->readable = false;
        $this->socketSendRetry = 3;
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Close
     *
     * Close the connection(s). Must be called by the application
     * but could be added to the __destruct method too.
     */
    public function close()
    {
        $this->readable     = false;
        $this->responseSize = null;

        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    /**
     * Create socket
     *
     * Set up the socket connection if not yet done.
     *
     * @throws \Kafka\Exception
     *
     * @return Resource $socket
     */
    protected function createSocket()
    {
        if (!is_resource($this->socket)) {
            if (!$this->socket = @stream_socket_client(
                $this->connection->getConnectionString(),
                $errno,
                $errstr)
            ) {
                throw new \Kafka\Exception($errstr, $errno);
            }
            stream_set_timeout($this->socket, $this->connection->getTimeout());
            //stream_set_read_buffer($this->socket,  65535);
            //stream_set_write_buffer($this->socket, 65535);
        }

        return $this->socket;
    }

    /**
     * Send
     *
     * Send a bounded request.
     *
     * @param String  $requestData
     * @param Boolean $expectsResponse
     *
     * @throws \Kafka\Exception
     */
    final protected function send($requestData, $expectsResposne = true)
    {
        $retry = $this->socketSendRetry;
        while ($retry > 0) {
            if ($this->socket === null) {
                $this->createSocket();
            } elseif ($this->socket === false) {
                throw new \Kafka\Exception(
                    "Kafka channel could not be created."
                );
            }
            if ($this->readable) {
                throw new \Kafka\Exception(
                    "Kafka channel has incoming data."
                );
            }
            $requestSize = strlen($requestData);
            $written = @fwrite($this->socket, pack('N', $requestSize));
            $written += @fwrite($this->socket, $requestData);
            if ($written  != $requestSize + 4) {
                $this->close();
                if (--$retry <= 0) {
                    throw new \Kafka\Exception(
                        "Request written $written bytes, expected to send:"
                        . ($requestSize + 4)
                    );
                } else {
                    continue;
                }
            }
            $this->readable = $expectsResposne;
            break;
        }

        return true;
    }

    /**
     * Read
     *
     * @param Integer  $size
     * @param Resource $stream
     *
     * @throws \Kafka\Exception
     */
    final protected function read($size, $stream = null)
    {
        if ($stream === null) {
            if (!$this->readable) {
                throw new \Kafka\Exception(
                    "Kafka channel is not readable."
                );
            }
            $stream = $this->socket;
        }
        if ($stream === $this->socket && $this->responseSize < $size) {
            //flush remaining data
            $this->read($this->responseSize, $stream);
            $this->readable = false;
            $remaining = $this->responseSize;
            $this->responseSize = null;
            throw new \Kafka\Exception\EndOfStream(
                "Trying to read $size from $remaining remaining."
            );
        }

        $soFarRead = 0;
        $result = '';
        $retrying = false;
        while ($soFarRead < $size) {
            $packet = fread($stream, $size - $soFarRead);
            if ($packet === false) {
                $this->close();
                throw new \Kafka\Exception(
                    "Could not read from the kafka channel socket."
                );
            } elseif (!$packet) {
                throw new \Kafka\Exception\EndOfStream(
                    "No response data received from kafka broker."
                );
            } else {
                $packetSize = strlen($packet);
                $soFarRead += $packetSize;
                $result .= $packet;
                if ($stream === $this->socket) {
                    $this->readBytes += $packetSize;
                    $this->responseSize -= $packetSize;
                }
            }
            $retrying = true;
        }

        return $result;
    }

    /**
     * Has incoming data
     *
     * Every response handler has to call this method to validate state of the
     * channel and read standard kafka channel headers.
     *
     * @throws \Kafka\Exception
     *
     * @return Boolean
     */
    protected function hasIncomingData()
    {
        if (is_resource($this->innerStream)) {
            $this->readBytes = 0;

            return true;
        }

        if ($this->socket === null) {
            $this->createSocket();
        } elseif ($this->socket === false) {
            throw new \Kafka\Exception(
                "Kafka channel could not be created."
            );
        }

        if (!$this->readable) {
            throw new \Kafka\Exception(
                "Request has not been sent - maybe a connection problem."
            );
            $this->responseSize = null;
        }

        if ($this->responseSize === null) {
            $bytes32 = @fread($this->socket, 4);
            if (!$bytes32) {
                $this->close();
                throw new \Kafka\Exception\EndOfStream(
                    "Could not read kafka response header."
                );
            }
            $this->responseSize = current(unpack('N', $bytes32));

            // read corelation id
            $this->read(4, $this->socket);
        }

        //has the request been read completely ?
        if ($this->responseSize < 0) {
            throw new \Kafka\Exception(
                "Corrupt response stream!"
            );
        } elseif ($this->responseSize == 0) {
            $this->readable = false;
            $this->responseSize = null;

            return false;
        } else {
            $this->readBytes = 0;

            return true;
        }
    }

    /**
     * Get read bytes
     *
     * @return Integer
     */
    public function getReadBytes()
    {
        return $this->readBytes;
    }

    /**
     * Get remaining bytes
     *
     * @return Integer
     */
    public function getRemainingBytes()
    {
        return $this->readBytes;
    }


    /**
     * 0.8 has correlationId which will be passed back to the client
     */
    private function generateRequestId() {
        return ++$this->_correlationId;
    }

    protected function encodeRequestHeader($requestKey) {
        $data = pack('n', $requestKey); //short
        $data .= pack('n', \Kafka\Kafka::REQUEST_API_VERSION); //short
        $data .= pack('N', $this->generateRequestId());//int
        $data .= $this->writeString($this->connection->getClientid());

        return $data;
    }

    /**
     * Write string wire format
     *
     * write 2 byte size and the stream
     * @param String $message
     * @param String $format length prefix string format. see php pack()
     */
    protected function writeString($message, $format='n') {
        if (empty($message)) {
            if ($format == 'n') {
                $data = pack($format, 0xFFFF);// -1 short signed short
            } elseif ($format == 'N') {
                $data = pack($format, 0xFFFFFFFF);// -1 short signed short
            } else {
                $data = pack($format, -1);
            }
        } else {
            $data = pack($format, strlen($message)) . $message;
        }

        return $data;
    }

    /**
     * Read string format from kafka
     *
     * read 2 byte size and read the string
     * @param Resource $stream
     */
    private function readString($stream = null)
    {
        if ($stream === null) {
            $stream = $this->socket;
        }

        $len = current(unpack('n', $this->read(2, $stream)));
        return $this->read($len, $stream);
    }

    /**
     * Get metadata information of a topic or multiple topic
     *
     * @param Mixed $topics single string topcis or array of topics
     */
    public function getTopicMetadata($topics)
    {
        $data = $this->encodeRequestHeader(\Kafka\Kafka::REQUEST_KEY_METADATA);

        if (!is_array($topics)) {
            $topics = array($topics);
        }

        $data .= pack('N', count($topics));;

        foreach ($topics as $topic) {
            $data .= $this->writeString($topic);
        }

        if ($this->send($data, true)) {
            return $this->loadMetadataResponse();
        } else {
            throw new \Kafka\Exception("Failed to send metadata to brooker");
        }
    }

    /**
     * Load metadata from a stream
     *
     * This will also update current $brokerList information
     *
     * @param Resource $stream
     */
    protected function loadMetadataResponse($stream = null)
    {
        if ($stream === null) {
            $stream = $this->socket;
        }

        if (!$this->hasIncomingData()) {
            throw new \Kafka\Exception("Failed to get metadata response from the broker");
        }

        $metadata = array();

        // Read Broker
        $brokers = array();
        $numBroker = current(unpack('N', $this->read(4, $stream)));
        for ($i=0; $i<$numBroker; $i++) {
            $broker = array();
            $brokerId = current(unpack('N', $this->read(4, $stream)));

            $broker['host'] = $this->readString($stream);
            $broker['port'] = current(unpack('N', $this->read(4, $stream)));

            $brokers[$brokerId] = $broker;
            $this->brokerList[$brokerId] = $broker;
        }
        $metadata['brokers'] = $brokers;

        // Read TopicMetadata
        $topics = array();
        $numTopic = current(unpack('N', $this->read(4, $stream)));
        for ($i=0; $i<$numTopic; $i++) {
            $topicMetadata = array();
            $topicMetadata['error_code'] = current(unpack('n', $this->read(2, $stream)));
            $topicName = $this->readString($stream);

            $partitions = array();
            $numPartition = current(unpack('N', $this->read(4, $stream)));
            for ($j=0; $j<$numPartition; $j++) {
                $partitionMetadata = array();
                $partitionMetadata['error_code'] = current(unpack('n', $this->read(2, $stream)));

                $partitionId = current(unpack('N', $this->read(4, $stream)));

                $partitionMetadata['leader_id'] = current(unpack('N', $this->read(4, $stream)));

                $numReplica = current(unpack('N', $this->read(4, $stream)));
                $partitionMetadata['replicas'] = unpack("N". $numReplica, $this->read(4 * $numReplica, $stream));

                $numIsr = current(unpack('N', $this->read(4, $stream)));
                $partitionMetadata['isr'] = unpack("N". $numReplica, $this->read(4 * $numReplica, $stream));

                $partitions[$partitionId] = $partitionMetadata;
            }
            $topicMetadata['partitions'] = $partitions;
            $topics[$topicName] = $topicMetadata;

        }
        $metadata['topics'] = $topics;

        return $metadata;
    }

    /**
     * Load Response after producing a message
     *
     * @param Resource $stream
     */
    protected function loadProduceResponse($stream = null)
    {
        if ($stream === null) {
            $stream = $this->socket;
        }

        if (!$this->hasIncomingData()) {
            throw new \Kafka\Exception("Failed to get metadata response from the broker");
        }

        $numTopic = current(unpack('N', $this->read(4, $stream)));

        $response = array();
        for ($i=0; $i<$numTopic; $i++) {
            $topic = $this->readString($stream);

            $numPartition = current(unpack('N', $this->read(4, $stream)));
            for ($j=0; $j<$numPartition; $j++) {
                $partition = current(unpack('N', $this->read(4, $stream)));
                $errorCode = current(unpack('n', $this->read(2, $stream)));
                $offset = new \Kafka\Offset_64bit();
                $offset->setData($this->read(8, $stream));

                $response[$topic][$partition]['error_code'] = $errorCode;
                $response[$topic][$partition]['offset'] = $offset;
            }
        }

        return $response;

    }


    /**
     * Pack message
     *
     * Internal method for packing message into kafka wire format.
     *
     * @param Message $message
     * @param Mixed   $overrideCompression Null or \Kafka\Kafka::COMPRESSION_NONE
     * or
     *      \Kafka\Kafka::COMPRESSION_GZIP, etc.
     *
     * @throws \Kafka\Exception
     */
    protected function packMessage( Message $message, $overrideCompression = null)
    {
        $compression = $overrideCompression === null
            ? $message->compression()
            : $overrideCompression;

        $messageKey = $message->key();
        switch ($compression) {
            case \Kafka\Kafka::COMPRESSION_NONE:
                $compressedPayload = $message->payload();
                break;
            case \Kafka\Kafka::COMPRESSION_GZIP:
                $compressedPayload = gzencode($message->payload());
                break;
            case \Kafka\Kafka::COMPRESSION_SNAPPY:
                throw new \Kafka\Exception(
                    "Snappy compression not yet implemented in php
                    client"
                );
                break;
            default:
                throw new \Kafka\Exception(
                    "Unknown kafka compression codec $compression"
                );
                break;
        }

        // for reach message using MAGIC_1 format which includes compression
        // attribute byte
        $data = pack('C', \Kafka\Kafka::MAGIC_0);//byte
        $data .= pack('C', $compression);//byte
        $data .= $this->writeString($messageKey, 'N');
        $data .= $this->writeString($compressedPayload, 'N');
        $data = pack('N', crc32($data)) . $data;//int

        $payload = pack('N', 0) . pack('N', 0); // 64 bit Offset
        $payload .= pack('N', strlen($data));
        $payload .= $data;

        return $payload;
    }

    private function strToHex($string)
    {
        $hex = '';
        for ($i=0; $i<strlen($string); $i++){
            $ord = ord($string[$i]);
            $hexCode = dechex($ord);

            if ($i != 0) {
                $hex .= ':';
            }
            $hex .= substr('0'.$hexCode, -2);
        }
        return $hex;
    }

}
