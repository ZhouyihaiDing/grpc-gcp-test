<?php
/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
namespace Grpc\GCP;

use Psr\Cache\CacheItemPoolInterface;

class Config
{
    private $hostname;
    private $gcp_call_invoker;

    public function __construct($hostname, $conf = null, CacheItemPoolInterface $cacheItemPool = null)
    {
        if ($conf == null) {
            // If there is no configure file, use the default gRPC channel.
            $this->gcp_call_invoker = new \Grpc\DefaultCallInvoker();
            return;
        }
        $gcp_channel = null;
        // $hostname is used for distinguishing different cloud APIs.
        $this->hostname = $hostname;
        $channel_pool_key = $hostname . 'gcp_channel' . getmypid();
        if (!$cacheItemPool) {
            // If there is no cacheItemPool, use shared memory for
            // caching the configuration and channel pool.
            $channel_pool_key = intval(base_convert(sha1($channel_pool_key), 16, 10));
            $shm_id = shm_attach(getmypid());
            $var1 = @shm_get_var($shm_id, $channel_pool_key);
            if ($var1) {
                $gcp_call_invoker = unserialize($var1);
            } else {
                $affinity_conf = $this->parseConfObject($conf);
                $gcp_call_invoker = new GCPCallInvoker($affinity_conf);
            }
            $this->gcp_call_invoker = $gcp_call_invoker;

            register_shutdown_function(function ($gcp_call_invoker, $channel_pool_key, $shm_id) {
                // Push the current gcp_channel back into the pool when the script finishes.
                if (!shm_put_var($shm_id, $channel_pool_key, serialize($gcp_call_invoker))) {
                    echo "[warning]: failed to update the item pool\n";
                }
            }, $gcp_call_invoker, $channel_pool_key, $shm_id);
        } else {
            $item = $cacheItemPool->getItem($channel_pool_key);
            if ($item->isHit()) {
                // Channel pool for the $hostname API has already created.
                $gcp_call_invoker = unserialize($item->get());
            } else {
                $affinity_conf = $this->parseConfObject($conf);
                // Create GCP channel based on the information.
                $gcp_call_invoker = new GCPCallInvoker($affinity_conf);
            }
            $this->gcp_call_invoker = $gcp_call_invoker;
            register_shutdown_function(function ($gcp_call_invoker, $channel_pool_key, $cacheItemPool, $item) {
                // Push the current gcp_channel back into the pool when the script finishes.
                $item->set(serialize($gcp_call_invoker));
                $cacheItemPool->save($item);
            }, $gcp_call_invoker, $channel_pool_key, $cacheItemPool, $item);
        }
    }

    public function callInvoker()
    {
        return $this->gcp_call_invoker;
    }

    private function parseConfObject($conf_object)
    {
        $config = json_decode($conf_object->serializeToJsonString(), true);
        $affinity_conf['channelPool'] = $config['channelPool'];
        $aff_by_method = array();
        for ($i = 0; $i < count($config['method']); $i++) {
            // In proto3, if the value is default, eg 0 for int, it won't be serialized.
            // Thus serialized string may not have `command` if the value is default 0(BOUND).
            if (!array_key_exists('command', $config['method'][$i]['affinity'])) {
                $config['method'][$i]['affinity']['command'] = 'BOUND';
            }
            $aff_by_method[$config['method'][$i]['name'][0]] = $config['method'][$i]['affinity'];
        }
        $affinity_conf['affinity_by_method'] = $aff_by_method;
        return $affinity_conf;
    }
}
