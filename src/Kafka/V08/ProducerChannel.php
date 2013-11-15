<?php
/**
 * Kafka 0.7 Producer Channel.
 *
 * In this version there is no acknowledgement that the message has
 * been received by the broker so the success is measured only
 * by writing succesfully into the socket.
 *
 * The channel implementation, however is the same as in 0.7
 *
 * @author michal.harish@gmail.com
 */

namespace Kafka\V08;

use Kafka\Kafka;
use Kafka\Message;
use Kafka\Iproducer;

require_once 'Channel.php';
class ProducerChannel
    extends Channel implements IProducer
{
    /**
     * @var array
     */
    private $messageQueue;

    /**
     * Required acknowledgement from brokker
     *
     * This field indicates how many acknowledgements the servers
     * should receive before responding to the requet:
     * 0 -> dont wait for acks
     * 1 -> wait until leader commit the message
     * n -> n > 1, wait for n broker to commit
     * -1 -> wait untill all replcia commit the message
     * @var Integer
     */
    private $requiredAcks = \Kafka\Kafka::REQUEST_ACK_LEADER;

    private $ackTimeout = 1000; // 1 sec

    /**
     * @param Kafka $connection
     * @param $requiredAcks @see requireAcks
     */
    public function __construct(Kafka $connection, $requiredAcks = \Kafka\Kafka::REQUEST_ACK_LEADER)
    {
        parent::__construct($connection);
        $this->messageQueue = array();
        $this->requiredAcks = $requiredAcks;
    }

    /**
     * Add a single message to the produce queue.
     *
     * @param  Message|string $message
     * @return boolean        Success
     */
    public function add(Message $message)
    {
        $this->messageQueue
            [$message->topic()]
            [$message->partition()]
            [] = $message;
    }


    /**
     * Produce all messages added.
     * @throws \Kafka\Exception On Failure
     * @return TRUE             On Success
     */
    public function produce()
    {

        if (count($this->messageQueue) == 0) {
            return true;
        }

        foreach ($this->messageQueue as $topic => &$partitions) {
            $metadata = $this->getTopicMetadata($topic);
            $topicMetadata = $metadata['topics'][$topic];

            foreach ($partitions as $partition => &$messageSet) {
                $leaderId = @$topicMetadata['partitions'][$partition]['leader_id'];
                // send message to the leader

                // create message set
                $data = $this->encodeRequestHeader(\Kafka\Kafka::REQUEST_KEY_PRODUCE);
                $data .= pack('n', $this->requiredAcks);
                $data .= pack('N', $this->ackTimeout);

                $data .= pack('N', 1); // num topicsc
                $data .= $this->writeString($topic);
                $data .= pack('N', 1); // num partition
                $data .= pack('N', $partition);

                $messageSet = $this->packMessageSet($messageSet);
                $data .= pack('N', strlen($messageSet)); // msg set size
                $data .= $messageSet;


                if ($this->hasIncomingData()) {
                    throw new \Kafka\Exception(
                        "The channel has incomming data"
                    );
                }

                $expectsResposne = $this->requiredAcks > 0;
                if ($this->send($data, $expectsResposne)) {
                    if ($expectsResposne) {
                        $response = $this->loadProduceResponse();
                        $errorCode = $response[$topic][$partition]['error_code'];
                        if ($errorCode != 0) {
                            throw \Kafka\Exception::createException($errorCode);
                        }
                    }
                    unset($partitions[$partition]);
                }
                unset($messageSet);
            }
            unset($partitions);
        }

        return true;
    }

    /**
     * Pack multiple message into the MessageSet protocol
     *
     * https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
     *
     * @param Array $mssages array of Kafka\Message
     * @return binary representation of mesage set
     */
    private function packMessageSet($messages)
    {

        $data = '';
        foreach($messages as &$message) {
            $data .= $this->packMessage($message);
        }

       return $data;
    }

}
