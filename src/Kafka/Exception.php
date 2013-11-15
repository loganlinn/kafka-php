<?php

namespace Kafka;

class Exception extends \RuntimeException
{
    /**
     * Create Response exception from kafka server
     */
    public static function createException(
        $errorNo,
        $message = null,
        $code = 0,
        Excpetion $previous = null
    )
    {
        switch($errorNo) {
        case -1:
            $errorClass = \Kafka\Unknown;
        case 1:
            $errorClass = \Kafka\OffsetOutOfRange;
        case 2:
            $errorClass = \Kafka\InvalidMessage;
        case 3:
            $errorClass = \Kafka\UnknownTopicOrPartition;
        case 4:
            $errorClass = \Kafka\InvalidMessageSize;
        case 5:
            $errorClass = \Kafka\LeaderNotAvailable;
        case 6:
            $errorClass = \Kafka\NotLeaderForPartition;
        case 7:
            $errorClass = \Kafka\RequestTimedOut;
        case 8:
            $errorClass = \Kafka\BrokerNotAvailable;
        case 9:
            $errorClass = \Kafka\ReplicaNotAvailable;
        case 10:
            $errorClass = \Kafka\MessageSizeTooLarge;
        case 11:
            $errorClass = \Kafka\StaleControllerEpochCode;
        case 12:
            $errorClass = \Kafka\OffsetMetadataTooLargeCode;
        default:
            return new self("Unknwon response error code number exception");
        }

        return new $errorClass($message, $code, $previous);
    }
}

namespace Kafka\Exception;

class EndOfStream extends \Kafka\Exception
{
}

class TopicUnavailable extends \Kafka\Exception
{
}

class Unknown extends \Kafka\Exception
{
}

class OffsetOutOfRange extends \Kafka\Exception
{
}

class InvalidMessage extends \Kafka\Exception
{
}
class UnknownTopicOrPartition extends \Kafka\Exception
{
}
class InvalidMessageSize extends \Kafka\Exception
{
}
class LeaderNotAvailable extends \Kafka\Exception
{
}
class NotLeaderForPartition extends \Kafka\Exception
{
}
class RequestTimedOut extends \Kafka\Exception
{
}
class BrokerNotAvailable extends \Kafka\Exception
{
}
class ReplicaNotAvailable extends \Kafka\Exception
{
}
class MessageSizeTooLarge extends \Kafka\Exception
{
}
class StaleControllerEpochCode extends \Kafka\Exception
{
}
class OffsetMetadataTooLargeCode extends \Kafka\Exception
{
}
