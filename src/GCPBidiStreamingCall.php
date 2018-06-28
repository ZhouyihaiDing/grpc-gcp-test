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

class GCPBidiStreamingCall extends GcpBaseCall
{
    private $first_rpc = null;
    private $metadata_rpc = null;
    private $response = null;

    private function createRealCall($channel)
    {
        $this->real_call = new \Grpc\BidiStreamingCall($channel, $this->method, $this->deserialize, $this->options);
        return $this->real_call;
    }

    public function start(array $metadata = [])
    {
        $this->metadata_rpc = $metadata;
    }

    public function read()
    {
        $response = $this->real_call->read();
        if ($response) {
            $this->response = $response;
        }
        return $response;
    }

    public function write($data, array $options = [])
    {
        if (!$this->first_rpc) {
            $this->first_rpc = $data;
            $channel_ref = $this->_rpcPreProcess($data);
            $this->createRealCall($channel_ref->getRealChannel(
                $this->gcp_channel->credentials));
            $this->real_call->start($this->metadata_rpc);
        }
        $this->real_call->write($data, $options);
    }

    public function writesDone()
    {
        $this->real_call->writesDone();
    }

    public function getStatus()
    {
        $status = $this->real_call->getStatus();
        $this->_rpcPostProcess($status, $this->response);
        return $status;
    }
}
