<?php

namespace Kommuna;

use \Memcached;

class MemcachedMT {

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

        $this->connect();

    }

    protected static function increaseConnectionRetry() {

        self::$connectionRetry =+1;

        if(self::$connectionRetry > 10) {
            throw new \Exception("Count of failed memcached connections > 10");
        }

    }

    protected function connect($reconectFlag = false) {

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
            $memcache->setOption(Memcached::OPT_RECV_TIMEOUT, Memcached::SERIALIZER_JSON);

        }

        self::increaseConnectionRetry();

        $this->memcached = $memcache;

    }

    public function set($key, $value, $ttl) {

        $result = $this->memcached->set($key, $value, $ttl);

        if(in_array($this->memcached->getResultCode(), self::$fatalCodes)) {

            $this->connect(true);
            $result = $this->set($key, $value, $ttl);

        }

        return $result;
    }

    public function get($key) {

        $result = $this->memcached->get($key);

        return $result;
    }

    public function delete($key, $time = 0) {

        return $this->memcached->delete($key, $time);

    }

    public function resultNotFound() {
        return $this->memcached->getResultCode() === Memcached::RES_NOTFOUND;
    }

}