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
namespace Google\Cloud\Grpc;

class GCPCallInvoker implements \Grpc\CallInvoker
{
    private $channel;
    private $affinity_conf;

    public function __construct($affinity_conf)
    {
        $this->affinity_conf = $affinity_conf;
    }

    public function createChannelFactory($hostname, $opts)
    {
        if ($this->channel) {
            // $call_invoker object has already created from previews PHP-FPM scripts.
            // Only need to udpate the $opts including the credentials.
            $this->channel->updateOpts($opts);
        } else {
            $opts['affinity_conf'] = $this->affinity_conf;
            $channel = new \Google\Cloud\Grpc\GcpExtensionChannel($hostname, $opts);
            $this->channel = $channel;
        }
        return $this->channel;
    }

    // _getChannel is used for testing only.
    public function _getChannel()
    {
        return $this->channel;
    }

    public function UnaryCall($channel, $method, $deserialize, $options)
    {
        return new GCPUnaryCall($channel, $method, $deserialize, $options);
    }
    public function ClientStreamingCall($channel, $method, $deserialize, $options)
    {
        return new GCPClientStreamCall($channel, $method, $deserialize, $options);
    }
    public function ServerStreamingCall($channel, $method, $deserialize, $options)
    {
        return new GCPServerStreamCall($channel, $method, $deserialize, $options);
    }
    public function BidiStreamingCall($channel, $method, $deserialize, $options)
    {
        return new GCPBidiStreamingCall($channel, $method, $deserialize, $options);
    }
}
