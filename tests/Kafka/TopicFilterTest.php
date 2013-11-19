<?php

require_once __DIR__ . "/../../src/Kafka/Kafka.php";

$topics = array(
    'topic',
    'topic1',
    'topic2',
    'topictest',
    'useractivity',
    'pageviews',
    'datasync'
);

$filter = new \Kafka\Whitelist(".*");
assert($filter->getTopics($topics) === $topics);

$filter = new \Kafka\Whitelist("topic.");
assert($filter->getTopics($topics) === array('topic1','topic2'));

$filter = new \Kafka\Whitelist("topic.+");
assert($filter->getTopics($topics) === array('topic1','topic2','topictest'));

$filter = new \Kafka\Whitelist("topic.*");
assert($filter->getTopics($topics) === array('topic','topic1','topic2','topictest'));


$filter = new \Kafka\Blacklist("topic.");
assert($filter->getTopics($topics) === array('topic','topictest','useractivity','pageviews','datasync'));

$filter = new \Kafka\Blacklist("topic.+");
assert($filter->getTopics($topics) === array('topic','useractivity','pageviews','datasync'));

$filter = new \Kafka\Blacklist("topic.*");
assert($filter->getTopics($topics) === array('useractivity','pageviews','datasync'));

try {
    $filter = new \Kafka\Blacklist(".*");
    assert($filter->getTopics($topics) === array()); //should trhow exceptions

    throw new Exception("testcase failed"); // if it reaches this line something is wrong
} catch(Exception $e) {
    $message = $e->getMessage();
    if ($e == "testcase failed") {
        throw $e;
    }
}
