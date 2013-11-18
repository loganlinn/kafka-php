<?php
/**
 * IMetadata implementation for 0.9.x Kafka prototcol
 *
 * This class will communicate to broker to get metadata using Channel
 *
 * @author    Michal Harish <michal.harish@gmail.com>
 * @author    Ahmy yulrizka <yulrizka@gmail.com>
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 */
namespace Kafka\V08;

use Kafka\Kafka;

class Metadata implements \Kafka\IMetadata
{
    private $channel;

    /**
     * @var null|array
     */
    private $brokerMetadata;


    private $topicsMetadata;

    public function __construct($channel)
    {
        $this->channel = $channel;
        $this->topicsMetadata = array();
    }

    /**
     * @return array[<topic>][<virutalPartition>] = array('broker'= <brokerId>, 'partition' = <brokerPartition>)
     */
    public function getTopicMetadata($topics = null)
    {
        if ($topics == null) {
            return false;
        }

        $data = $this->channel->encodeRequestHeader(\Kafka\Kafka::REQUEST_KEY_METADATA);

        if (!is_array($topics)) {
            $topics = array($topics);
        }

        $data .= pack('N', count($topics));;

        foreach ($topics as $topic) {
            $data .= $this->channel->writeString($topic);
        }

        if ($this->channel->send($data, true)) {
            $metadata =  $this->loadMetadataResponse();
        } else {
            throw new \Kafka\Exception("Failed to send metadata to brooker");
        }

        $this->brokerMetadata = $metadata['brokers'];
        $this->topicsMetadata = array_merge($this->topicsMetadata, $metadata['topics']);

        return $metadata['topics'];
    }

    /**
     * @param  int            $brokerId
     * @return array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    public function getBrokerInfo($brokerId)
    {
        throw new \Kafka\Exception("Not implemented");
    }

    /**
     * @return array[<brokerId>] => array('name'=> ..., 'host'=>..., 'port' =>... )
     */
    function getBrokerMetadata()
    {
        //TODO: handle when brokermetadata is null
        return $this->brokerMetadata;
    }

    /**
     * @return boolean
     */
    function needsRefereshing() {
        return $this->brokerMetadata === null /*|| $this->topicMetadata === null*/;
    }


    /**
     * @param String $groupid
     * @param String $processId
     */
    function registerConsumerProcess($groupId, $processId)
    {
        throw new \Kafka\Exception("Not implemented");
    }

    /**
     * @param String $groupId
     * @param String $topic
     */
    function getTopicOffsets($groupId, $topic)
    {
        throw new \Kafka\Exception("Not implemented");
    }

    function commitOffset($groupId, $topic, $brokerId, $partition, \Kafka\Offset $offset)
    {
        throw new \Kafka\Exception("Not implemented");
    }

    /**
     * Load metadata from a stream
     *
     * This will also update current $brokerList information
     *
     * @param Resource $stream
     * @retrun Mixed Array of metadata if success or false
     */
    function loadMetadataResponse()
    {
        if (!$this->channel->hasIncomingData()) {
            throw new \Kafka\Exception("Failed to get metadata response from the broker");
        }

        $metadata = array();

        // Read Broker
        $brokers = array();
        $numBroker = current(unpack('N', $this->channel->read(4)));
        for ($i=0; $i<$numBroker; $i++) {
            $broker = array();
            $brokerId = current(unpack('N', $this->channel->read(4)));

            $broker['host'] = $this->channel->readString();
            $broker['port'] = current(unpack('N', $this->channel->read(4)));

            $brokers[$brokerId] = $broker;
            $this->brokerList[$brokerId] = $broker;
        }
        $metadata['brokers'] = $brokers;

        // Read TopicMetadata
        $topics = array();
        $numTopic = current(unpack('N', $this->channel->read(4)));
        for ($i=0; $i<$numTopic; $i++) {
            $topicMetadata = array();
            $topicMetadata['error_code'] = current(unpack('n', $this->channel->read(2)));
            $topicName = $this->channel->readString();

            $partitions = array();
            $numPartition = current(unpack('N', $this->channel->read(4)));
            for ($j=0; $j<$numPartition; $j++) {
                $partitionMetadata = array();
                $partitionMetadata['error_code'] = current(unpack('n', $this->channel->read(2)));

                $partitionId = current(unpack('N', $this->channel->read(4)));

                $partitionMetadata['leader_id'] = current(unpack('N', $this->channel->read(4)));

                $numReplica = current(unpack('N', $this->channel->read(4)));
                for ($k=0; $k<$numReplica; $k++) {
                $partitionMetadata['replicas'][] = unpack("N", $this->channel->read(4));

                }

                $numIsr = current(unpack('N', $this->channel->read(4)));
                for ($k=0; $k<$numIsr; $k++) {
                    $partitionMetadata['isr'][] = unpack("N", $this->channel->read(4));

                }

                $partitions[$partitionId] = $partitionMetadata;
            }
            $topicMetadata['partitions'] = $partitions;
            $topics[$topicName] = $topicMetadata;

        }
        $metadata['topics'] = $topics;

        if (!$this->channel->hasIncomingData()) {
            return $metadata;
        }

        return false;
    }


}
