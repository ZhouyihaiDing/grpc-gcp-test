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

class GCPUnaryCall extends GcpBaseCall
{
    private function createRealCall($channel)
    {
        $this->real_call = new \Grpc\UnaryCall($channel, $this->method, $this->deserialize, $this->options);
        return $this->real_call;
    }

    // Public funtions are rewriting all methods inside UnaryCall
    public function start($argument, $metadata, $options)
    {
        $channel_ref = $this->_rpcPreProcess($argument);
        $real_channel = $channel_ref->getRealChannel($this->gcp_channel->credentials);
        $this->createRealCall($real_channel);
        $this->real_call->start($argument, $metadata, $options);
    }

    public function wait()
    {
        list($response, $status) = $this->real_call->wait();
        $this->_rpcPostProcess($status, $response);
        return [$response, $status];
    }

    public function getMetadata()
    {
        return $this->real_call->getMetadata();
    }
}
