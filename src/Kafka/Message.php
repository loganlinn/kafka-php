<?php

/**
 * Kafka message
 *
 * Kafka Message object used both for producing and conusming messages.
 * Handles format detection from the stream as well as compression/decompression
 * of the payload and crc validation.
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */

namespace Kafka;

use Kafka\Offset;

class Message
{
    private $topic;
    private $partition;
    private $offset;
    private $compression;
    private $payload;
    private $key;

    /**
     * Constructor is private used by the static creator methods below.
     *
     * @param string $topic
     * @param int    $partition
     * @param string $payload
     * @param int    $compression
     * @param Offset $offset
     * @param key    $key
     *
     * @throws \Kafka\Exception
     */
    public function __construct(
        $topic,
        $partition,
        $payload,
        $compression = \Kafka\Kafka::COMPRESSION_NONE,
        Offset $offset = NULL,
        $key = NULL
    )
    {
        if (!$topic) {
            throw new \Kafka\Exception("Topic name cannot be an empty string.");
        }
        $this->topic = $topic;
        if (!is_numeric($partition) || $partition < 0) {
            throw new \Kafka\Exception(
                "Partition must be a positive integer or 0."
            );
        }
        $this->topic = $topic;
        $this->partition = $partition;
        if ($offset === NULL) {
            $offset = new Offset();
        }
        $this->offset = $offset;
        $this->compression = $compression;
        $this->payload = $payload;
        $this->key = $key;
    }

    /**
     * @return string
     */
    final public function topic()
    {
        return $this->topic;
    }

    /**
     * @return partition
     */
    final public function partition()
    {
        return $this->partition;
    }

    /**
     * Final value of the uncompressed payload
     * @return string
     */
    final public function payload()
    {
        return $this->payload;
    }

    /**
     * Final value for the key
     * @return string
     */
    final public function key() {
        return $this->key;
    }

    /**
     * @return int
     */
    final public function compression()
    {
        return $this->compression;
    }

    /**
     * Final information about the message offset in the broker log.
     * @return Offset
     */
    final public function offset()
    {
        return clone $this->offset;
    }
}
