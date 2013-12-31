<?php
/**
 * System test for sending messages to php and check if consumer
 * received all of the message in the correct order.
 * This was design for 0.8, and at this point the consumer is not available.
 * so this test only produce the message
 */

require_once __DIR__ . "/../../src/Kafka/Kafka.php";

$brokers = '127.0.0.1:9092';
$timeout = 2;
$protocol = 0.8;
$topic = 'test';
$partition = 0;

$numMessages = 1000;

try {
	global $kafka,$consumer;
    $kafka = new \Kafka\Kafka($brokers, $timeout, $protocol);
    $producer = $kafka->createProducer(\Kafka\Kafka::REQUEST_ACK_NONE);

    //sendBatch($producer, $numMessages, 100);
    sendSingle($producer, $numMessages);


} catch (Exception $e) {
    throw $e;
}

function sendBatch($producer, $numMessages, $perBatch) {
    global $topic, $partition;

    // try send message in 5 batches
    $produced = 0;
    $started = microtime(true);
    // test sending message in single packet
    for ($i = 0; $i < $numMessages; $i++) {
        $producer->add(
            new \Kafka\Message(
                $topic,
                $partition,
                $i,
                \Kafka\Kafka::COMPRESSION_NONE
            )
        );
        if ($i % $perBatch == 0) {
            if ($producer->produce())
            {
                $produced++;
            } else {
                echo "Failed to send message $i\n";
            }
        }

        if ($i % 100 == 0) {
            echo "sent $i\n";
        }
    }

    // send remaining message
    if ($producer->produce())
    {
        $produced++;
    } else {
        echo "Failed to send message $i\n";
    }

    $elapsed = microtime(true) - $started;
    echo "sent $produced in $elapsed seconds\n";
}

function sendSingle($producer, $numMessages) {
    global $topic, $partition;
    $produced = 0;
    $started = microtime(true);
    // test sending message in single packet
    for ($i = 0; $i < $numMessages; $i++) {
        $producer->add(
            new \Kafka\Message(
                $topic,
                $partition,
                $i,
                \Kafka\Kafka::COMPRESSION_NONE
            )
        );
        if ($producer->produce())
        {
            $produced++;
        } else {
            echo "Failed to send message $i\n";
        }
        if ($i % 100 == 0) {
            echo "sent $i\n";
        }
    }

    return;
    $elapsed = microtime(true) - $started;
    echo "sent $produced in $elapsed seconds\n";
}
