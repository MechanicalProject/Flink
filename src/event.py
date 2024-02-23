from pyflink.common import Time
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import StateTtlConfig

import json

class PreprocessEvent(KeyedProcessFunction):
    def __init__(self):
        pass

    def create_value_state_descriptor(self):
        pass

    @staticmethod
    def _get_ttl_config(time: Time) -> StateTtlConfig:
        ttl_config = (
            StateTtlConfig.new_builder(time)
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        return ttl_config

    def open(self, runtime_context):
        pass

    def process_element(self, value, ctx):

        key = ctx.get_current_key()
        print("coin :", key)

        print("value :",value)
        # # Event
        price = eval(json.loads(value))['Symbol']
        
        print(type(price))
        print(price)

        yield str({"BTC_PRICE": price})
