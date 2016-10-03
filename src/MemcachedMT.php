<?php

namespace Kommuna;

use \Memcached;
use PinbaTrait\PinbaTrait;

class MemcachedMT {

    use PinbaTrait;

    public $memcached;


    protected static $instance;
    protected static $connectionRetry = 0;
    protected static $fatalCodes = [
        Memcached::RES_ERRNO,
        Memcached::RES_TIMEOUT,
        Memcached::RES_HOST_LOOKUP_FAILURE,
        Memcached::RES_CONNECTION_SOCKET_CREATE_FAILURE,
        Memcached::RES_SERVER_MARKED_DEAD,
        3, // MEMCACHED_CONNECTION_FAILURE
        47, // MEMCACHED_SERVER_TEMPORARILY_DISABLED
    ];

    protected static $settings = [];

    public function __construct($settings) {
        self::$settings = $settings;
        self::init(!empty($settings['pinba']) ? $settings['pinba'] : null);
        $this->connect();
    }

    protected static function increaseConnectionRetry() {

        self::$connectionRetry =+1;

        if(self::$connectionRetry > 10) {
            throw new \Exception("Count of failed memcached connections > 10");
        }

    }

    protected function connect($reconectFlag = false) {

        $pinba = self::pinba_timer_start('connect');

        $settings = self::$settings;

        if($reconectFlag) {
            $this->memcached->quit();
        }

        $memcache = new Memcached($settings['poolName']);

        $servers = $memcache->getServerList();
        $serversFromConfig = $settings['servers'];

        if (!$reconectFlag && count($servers) !== count($serversFromConfig)) {

            $reconectFlag = true;

        } else {
            for ($i = 0; $i < count($serversFromConfig); $i++) {
                if ("{$servers[$i]['host']}:{$servers[$i]['port']}" !== "{$serversFromConfig[$i]['host']}:{$serversFromConfig[$i]['port']}") {
                    $reconectFlag = true;
                    break;
                }
            }
        }


        if ($reconectFlag) {

            $memcache->resetServerList();
            $memcache->addServers($serversFromConfig);

            $memcache->setOption(Memcached::OPT_SERVER_FAILURE_LIMIT, $settings['options']['OPT_SERVER_FAILURE_LIMIT']);
            $memcache->setOption(Memcached::OPT_LIBKETAMA_COMPATIBLE,  true);
            $memcache->setOption(Memcached::OPT_REMOVE_FAILED_SERVERS, 12);
            $memcache->setOption(Memcached::OPT_RETRY_TIMEOUT,      1);
            $memcache->setOption(Memcached::OPT_CONNECT_TIMEOUT, 100); // miliseconds
            $memcache->setOption(Memcached::OPT_NO_BLOCK, 1);
            $memcache->setOption(Memcached::OPT_POLL_TIMEOUT, 100);    // miliseconds
            $memcache->setOption(Memcached::OPT_SEND_TIMEOUT, 100000); // microseconds
            $memcache->setOption(Memcached::OPT_RECV_TIMEOUT, 100000); // microseconds
            $memcache->setOption(Memcached::OPT_SERIALIZER, Memcached::SERIALIZER_PHP);

        }

        self::increaseConnectionRetry();

        self::pinba_timer_stop($pinba);

        $this->memcached = $memcache;

    }

    public function set($key, $value, $ttl) {

        $pinba = self::pinba_timer_start('set');

        $result = $this->memcached->set($key, $value, $ttl);

        if(in_array($this->memcached->getResultCode(), self::$fatalCodes)) {

            $this->connect(true);
            $result = $this->set($key, $value, $ttl);

        }
        self::pinba_timer_stop($pinba);

        return $result;
    }

    public function get($key) {

        $pinba = self::pinba_timer_start('get');

        $result = $this->memcached->get($key);
        self::pinba_timer_stop($pinba);

        return $result;
    }

    public function delete($key, $time = 0) {

        $pinba = self::pinba_timer_start('delete');

        $result = $this->memcached->delete($key, $time);

        self::pinba_timer_stop($pinba);

        return $result;

    }

    public function resultNotFound() {
        return $this->memcached->getResultCode() === Memcached::RES_NOTFOUND;
    }

}