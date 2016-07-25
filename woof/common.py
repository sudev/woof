class WoofConsumerError(RuntimeError):
    pass

class WoofNotSupported(WoofConsumerError):
    pass


CURRENT_PROD_BROKER_VERSION = '0.9'