<?php

namespace Bernard\Driver;

use Pheanstalk\Contract\PheanstalkInterface;

/**
 * Implements a Driver for use with https://github.com/pda/pheanstalk
 *
 * @package Bernard
 */
class PheanstalkDriver implements \Bernard\Driver
{
    protected $pheanstalk;

    /**
     * @param PheanstalkInterface $pheanstalk
     */
    public function __construct(PheanstalkInterface $pheanstalk)
    {
        $this->pheanstalk = $pheanstalk;
    }

    /**
     * {@inheritdoc}
     */
    public function listQueues()
    {
        return $this->pheanstalk->listTubes();
    }

    /**
     * {@inheritdoc}
     */
    public function createQueue($queueName)
    {
    }

    /**
     * {@inheritdoc}
     */
    public function countMessages($queueName)
    {
        $stats = $this->pheanstalk->statsTube($queueName);

        return $stats['current-jobs-ready'];
    }

    /**
     * {@inheritdoc}
     */
    public function pushMessage($queueName, $message)
    {
        $this->pheanstalk->useTube($queueName);
        $this->pheanstalk->put($message);
    }

    /**
     * {@inheritdoc}
     */
    public function popMessage($queueName, $duration = 5)
    {
        $job = $this->pheanstalk->withWatchedTube($queueName, 
                fn(Pheanstalk $ph) => $ph->reserveWithTimeout($duration)
        );
        if ($job) {
            return [$job->getData(), $job];
        }

        return array(null, null);
    }

    /**
     * {@inheritdoc}
     */
    public function acknowledgeMessage($queueName, $receipt)
    {
        $this->pheanstalk->delete($receipt);
    }

    /**
     * {@inheritdoc}
     */
    public function peekQueue($queueName, $index = 0, $limit = 20)
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function removeQueue($queueName)
    {
    }

    /**
     * {@inheritdoc}
     */
    public function info()
    {
        return $this->pheanstalk
            ->stats()
            ->getArrayCopy()
        ;
    }
}
