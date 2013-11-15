<?php

namespace Kafka;

class Exception extends \RuntimeException
{
    /**
     * Create Response exception from kafka server
     */
    public static function createResponseException(
        $errorNo,
        $message = null,
        $code = 0,
        Excpetion $previous = null
    )
    {
        switch($errorNo) {
        case -1:
            $error = new Exception\Unknown($message, $code, $previous);
            break;
        case 1:
            $error = new Exception\OffsetOutOfRange($message, $code, $previous);
            break;
        case 2:
            $error = new Exception\InvalidMessage($message, $code, $previous);
            break;
        case 3:
            $error = new Exception\UnknownTopicOrPartition($message, $code, $previous);
            break;
        case 4:
            $error = new Exception\InvalidMessageSize($message, $code, $previous);
            break;
        case 5:
            $error = new Exception\LeaderNotAvailable($message, $code, $previous);
            break;
        case 6:
            $error = new Exception\NotLeaderForPartition($message, $code, $previous);
            break;
        case 7:
            $error = new Exception\RequestTimedOut($message, $code, $previous);
            break;
        case 8:
            $error = new Exception\BrokerNotAvailable($message, $code, $previous);
            break;
        case 9:
            $error = new Exception\ReplicaNotAvailable($message, $code, $previous);
            break;
        case 10:
            $error = new Exception\MessageSizeTooLarge($message, $code, $previous);
            break;
        case 11:
            $error = new Exception\StaleControllerEpochCode($message, $code, $previous);
            break;
        case 12:
            $error = new Exception\OffsetMetadataTooLargeCode($message, $code, $previous);
            break;
        default:
            $error = new self("Unknwon response error code number exception");
        }

        return new $error;
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
