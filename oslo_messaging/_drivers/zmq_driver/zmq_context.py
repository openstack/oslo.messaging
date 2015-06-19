#    Copyright 2015 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


from oslo_messaging._drivers import common as rpc_common


class RpcContext(rpc_common.CommonRpcContext):
    """Context that supports replying to a rpc.call."""
    def __init__(self, **kwargs):
        self.replies = []
        super(RpcContext, self).__init__(**kwargs)

    def deepcopy(self):
        values = self.to_dict()
        values['replies'] = self.replies
        return self.__class__(**values)

    def reply(self, reply=None, failure=None, ending=False):
        if ending:
            return
        self.replies.append(reply)
