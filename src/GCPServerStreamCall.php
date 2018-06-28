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

class GCPServerStreamCall extends GcpBaseCall
{
    private $response = null;

    private function createRealCall($channel)
    {
        $this->real_call = new \Grpc\ServerStreamingCall($channel, $this->method, $this->deserialize, $this->options);
        return $this->real_call;
    }

    public function start($argument, $metadata, $options)
    {
        $channel_ref = $this->_rpcPreProcess($argument);
        $this->createRealCall($channel_ref->getRealChannel(
            $this->gcp_channel->credentials));
        $this->real_call->start($argument, $metadata, $options);
    }

    public function responses()
    {
        $response = $this->real_call->responses();
        // Since the last response is empty for the server streaming RPC,
        // the second last one is the last RPC response with payload.
        // Use this one for searching the affinity key.
        // The same as BidiStreaming.
        if ($response) {
            $this->response = $response;
        }
        return $response;
    }

    public function getStatus()
    {
        $status = $this->real_call->getStatus();
        $this->_rpcPostProcess($status, $this->response);
        return $status;
    }
}
